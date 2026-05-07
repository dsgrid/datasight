"""CLI command module."""

import asyncio
import json
import os
import sys
from pathlib import Path

import rich_click as click

from datasight.audit_report import (
    build_audit_report,
    render_audit_report_html,
    render_audit_report_markdown,
)
from datasight.data_profile import find_table_info
from datasight.validation import load_validation_config

from datasight import cli
from datasight.cli_helpers import format_epilog


@click.command(
    name="audit-report",
    epilog=format_epilog(
        """
        Examples:

            datasight audit-report
            datasight audit-report -o audit.html
            datasight audit-report --format markdown -o audit.md
            datasight audit-report --table generation_fuel -o generation-audit.html
        """
    ),
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Scope the audit to a specific table.")
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default="report.html",
    show_default=True,
    help="Output path (.html, .md, or .json).",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["html", "markdown", "json"]),
    default=None,
    help="Output format (default: inferred from file extension).",
)
def audit_report(project_dir, table, output_path, output_format):
    """Generate a comprehensive audit report combining all checks.

    Combines profile, measures, quality, integrity, distribution, and
    validation results into one HTML, Markdown, or JSON artifact.
    """
    project_dir = str(Path(project_dir).resolve())
    settings, _ = cli.resolve_settings(project_dir)
    resolved_db_path = cli.resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    # Infer format from extension if not specified
    if output_format is None:
        ext = Path(output_path).suffix.lower()
        if ext == ".html":
            output_format = "html"
        elif ext == ".md":
            output_format = "markdown"
        elif ext == ".json":
            output_format = "json"
        else:
            output_format = "html"

    from datasight.config import load_joins_config, load_measure_overrides

    measure_overrides = load_measure_overrides(None, project_dir)
    validation_rules = load_validation_config(None, project_dir) or None
    declared_joins = load_joins_config(None, project_dir) or None

    async def _run_audit_report():
        sql_runner, schema_info = await cli.load_schema_info_for_project(project_dir, settings)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info_filtered = [table_info]
        else:
            schema_info_filtered = schema_info
        return await build_audit_report(
            schema_info_filtered,
            sql_runner.run_sql,
            measure_overrides,
            validation_rules,
            declared_joins,
            project_name=Path(project_dir).name,
        )

    report_data = asyncio.run(_run_audit_report())

    if output_format == "json":
        cli.write_or_print(json.dumps(report_data, indent=2), output_path)
    elif output_format == "markdown":
        cli.write_or_print(render_audit_report_markdown(report_data), output_path)
    else:
        cli.write_or_print(render_audit_report_html(report_data), output_path)
