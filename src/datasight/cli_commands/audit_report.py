# ruff: noqa: F401, F403, F405
"""CLI command module."""

from datasight import cli as cli_root
from datasight.cli import *  # noqa: F403
from datasight.cli import (
    _build_metric_table,
    _build_profile_detail_table,
    _build_sql_script,
    _configure_logging,
    _current_db_settings_or_none,
    _default_chart_extension,
    _default_data_extension,
    _emit_ask_result,
    _emit_cli_provenance,
    _epilog,
    _fmt_dist,
    _format_profile_value,
    _iter_sql_tool_results,
    _load_batch_entries,
    _load_recipe_entries,
    _load_schema_info_for_project,
    _prepare_web_runtime,
    _print_sql_queries,
    _question_table_prefix,
    _render_dimensions_markdown,
    _render_distribution_markdown,
    _render_doctor_markdown,
    _render_integrity_markdown,
    _render_measures_markdown,
    _render_profile_markdown,
    _render_quality_markdown,
    _render_recipes_markdown,
    _render_trends_markdown,
    _render_validation_markdown,
    _resolve_db_path,
    _resolve_settings,
    _sanitize_sql_identifier,
    _slugify_filename,
    _sql_comment_lines,
    _validate_batch_entry,
    _validate_settings_for_llm,
    _write_batch_result_files,
    _write_or_print,
)


def create_llm_client(*args, **kwargs):
    return cli_root.create_llm_client(*args, **kwargs)


async def _run_ask_pipeline(*args, **kwargs):
    return await cli_root._run_ask_pipeline(*args, **kwargs)


@click.command(
    name="audit-report",
    epilog=_epilog(
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
    _configure_logging("INFO")
    project_dir = str(Path(project_dir).resolve())
    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
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
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
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
        _write_or_print(json.dumps(report_data, indent=2), output_path)
    elif output_format == "markdown":
        _write_or_print(render_audit_report_markdown(report_data), output_path)
    else:
        _write_or_print(render_audit_report_html(report_data), output_path)
