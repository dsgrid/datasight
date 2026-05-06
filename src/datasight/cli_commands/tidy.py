"""CLI command module."""

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import rich_click as click

from datasight.data_profile import find_table_info

from datasight import cli
from datasight.cli_helpers import format_epilog


def _project_scope_options(func):
    """Add the --project-dir and --table options shared by all tidy subcommands."""
    func = click.option(
        "--table",
        "source_table",
        default=None,
        help="Scope tidy detection to a specific source table.",
    )(func)
    func = click.option(
        "--project-dir",
        type=click.Path(exists=True),
        default=".",
        help="Project directory containing .env and config files.",
    )(func)
    return func


async def _gather_tidy_data(project_dir: str, source_table: str | None, settings):
    from datasight.tidy import _detect_period_groups, analyze_tidy_patterns

    sql_runner, schema_info = await cli.load_schema_info_for_project(project_dir, settings)
    try:
        if source_table:
            table_info = find_table_info(schema_info, source_table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {source_table}")
            schema_info = [table_info]
        data = analyze_tidy_patterns(schema_info)
        suggestions_by_table = {t["name"]: _detect_period_groups(t) for t in schema_info}
        return data, suggestions_by_table
    finally:
        sql_runner.close()


async def _gather_tidy_data_for_files(files: tuple[str, ...]):
    """Like ``_gather_tidy_data`` but registers ``files`` in an ephemeral DuckDB.

    No project / .env required; mirrors the file-mode entry point used by
    ``datasight inspect``. The returned ``suggestions_by_table`` is empty
    on this path because ephemeral file sessions can't usefully persist a
    reshape — callers should treat file mode as suggest-only.
    """
    from datasight.explore import create_files_session_for_settings
    from datasight.schema import introspect_schema
    from datasight.tidy import analyze_tidy_patterns

    runner, _tables_info = create_files_session_for_settings(list(files), None)
    try:
        tables = await introspect_schema(runner.run_sql, runner=runner)
        schema_info = [
            {
                "name": t.name,
                "row_count": t.row_count,
                "columns": [
                    {"name": c.name, "dtype": c.dtype, "nullable": c.nullable} for c in t.columns
                ],
            }
            for t in tables
        ]
        return analyze_tidy_patterns(schema_info)
    finally:
        runner.close()


def _resolve_tidy_settings(project_dir: str) -> tuple[Any, str, str]:
    project_dir = str(Path(project_dir).resolve())
    settings, _ = cli.resolve_settings(project_dir)
    resolved_db_path = cli.resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)
    return settings, project_dir, resolved_db_path


def _apply_reshapes(
    suggestions_by_table: dict[str, list[Any]],
    mode: str,
    dry_run: bool,
    resolved_db_path: str,
) -> list[dict[str, Any]]:
    """Apply (or preview) tidy suggestions and return a per-statement audit log."""
    ddl_statements: list[tuple[str, Any]] = [
        (source_table, suggestion)
        for source_table, suggestions in suggestions_by_table.items()
        for suggestion in suggestions
    ]
    applied: list[dict[str, Any]] = []
    if not ddl_statements:
        click.echo("No untidy column-shape patterns detected.")
        return applied

    if dry_run:
        for source_table, suggestion in ddl_statements:
            click.echo(f"-- Would apply for {source_table}:")
            click.echo(suggestion.build_sql(mode))
    else:
        import duckdb

        conn = duckdb.connect(resolved_db_path)
        try:
            for source_table, suggestion in ddl_statements:
                conn.execute(suggestion.build_sql(mode))
                click.echo(
                    f"Created {mode} {suggestion.target_object_name!r} "
                    f"from {source_table} ({len(suggestion.affected_columns)} columns)"
                )
        finally:
            conn.close()

    for source_table, suggestion in ddl_statements:
        applied.append(
            {
                "table": source_table,
                "target_object_name": suggestion.target_object_name,
                "object_type": mode,
                "affected_columns": suggestion.affected_columns,
                "dry_run": dry_run,
            }
        )
    return applied


@click.group(
    epilog=format_epilog(
        """
        Examples:

            datasight tidy suggest
            datasight tidy suggest --table sales_wide
            datasight tidy view --dry-run
            datasight tidy view
            datasight tidy table --table sales_wide
        """
    )
)
def tidy():
    """Detect untidy column shapes and reshape into long form.

    Use 'tidy suggest' to inspect candidates, 'tidy view' to create
    long-form views, or 'tidy table' to materialize long-form tables.
    Detection is deterministic — column names plus dtypes plus row counts —
    so no LLM is involved.
    """


@click.command(
    name="suggest",
    epilog=format_epilog(
        """
        Examples:

            datasight tidy suggest                           # current project
            datasight tidy suggest monthly_generation.csv    # standalone file
            datasight tidy suggest gen.csv plants.parquet    # multiple files
            datasight tidy suggest --table sales_wide
            datasight tidy suggest --format markdown -o tidy.md
        """
    ),
)
@click.argument("files", nargs=-1, required=False, type=click.Path(exists=True))
@_project_scope_options
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format (default: table).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Write the tidy listing to a file instead of stdout.",
)
def tidy_suggest(files, project_dir, source_table, output_format, output_path):
    """List detected untidy column shapes without changing the database.

    Pass one or more CSV / Parquet / Excel / DuckDB files as positional
    arguments to inspect them in an ephemeral session — no project setup
    required. With no files, runs against the current project's database.
    """
    from rich.console import Console

    if files:
        if source_table:
            raise click.UsageError(
                "--table cannot be combined with positional FILES; "
                "scope is implicit when files are passed."
            )
        tidy_data = asyncio.run(_gather_tidy_data_for_files(files))
    else:
        settings, project_dir, _ = _resolve_tidy_settings(project_dir)
        tidy_data, _ = asyncio.run(_gather_tidy_data(project_dir, source_table, settings))

    if output_format == "json":
        cli.write_or_print(json.dumps(tidy_data, indent=2), output_path)
        return

    if output_format == "markdown":
        cli.write_or_print(cli.render_tidy_markdown(tidy_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        cli.build_metric_table(
            "Tidy Reshape Suggestions",
            [
                ("Tables scanned", str(tidy_data["table_count"])),
                ("Suggestions", str(len(tidy_data.get("suggestions") or []))),
            ],
        )
    )
    suggestions = tidy_data.get("suggestions") or []
    if suggestions:
        console.print(
            cli.build_profile_detail_table(
                "Suggestions",
                [
                    ("Source", "left"),
                    ("Target", "left"),
                    ("Pattern", "left"),
                    ("Dimensions", "left"),
                    ("Columns", "right"),
                    ("Rationale", "left"),
                ],
                [
                    [
                        item["table"],
                        item["target_object_name"],
                        item["pattern"],
                        ", ".join(d["name"] for d in item["dimensions"]),
                        str(len(item["column_mappings"])),
                        item["rationale"],
                    ]
                    for item in suggestions
                ],
            )
        )
    wide_tables = tidy_data.get("wide_tables") or []
    if wide_tables:
        console.print(
            cli.build_profile_detail_table(
                "Wide Tables",
                [("Table", "left"), ("Columns", "right"), ("Rows", "right"), ("Reason", "left")],
                [
                    [
                        item["table"],
                        str(item["column_count"]),
                        str(item.get("row_count") if item.get("row_count") is not None else "?"),
                        item["reason"],
                    ]
                    for item in wide_tables
                ],
            )
        )
    if not suggestions and not wide_tables:
        console.print("[dim]No untidy column-shape patterns detected.[/dim]")
    if output_path:
        cli.write_or_print(console.export_text(), output_path)


@click.command(
    name="view",
    epilog=format_epilog(
        """
        Examples:

            datasight tidy view
            datasight tidy view --dry-run
            datasight tidy view --table sales_wide
        """
    ),
)
@_project_scope_options
@click.option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    default=False,
    help="Print the DDL without executing it.",
)
def tidy_view(project_dir, source_table, dry_run):
    """Create CREATE OR REPLACE VIEW <table>_long for each detected pattern."""
    settings, project_dir, resolved_db_path = _resolve_tidy_settings(project_dir)
    if settings.database.mode != "duckdb":
        raise click.UsageError(
            "tidy view requires DuckDB; the apply path opens a writable DuckDB connection."
        )
    _, suggestions_by_table = asyncio.run(_gather_tidy_data(project_dir, source_table, settings))
    _apply_reshapes(suggestions_by_table, "view", dry_run, resolved_db_path)


@click.command(
    name="table",
    epilog=format_epilog(
        """
        Examples:

            datasight tidy table
            datasight tidy table --dry-run
            datasight tidy table --table sales_wide
        """
    ),
)
@_project_scope_options
@click.option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    default=False,
    help="Print the DDL without executing it.",
)
def tidy_table(project_dir, source_table, dry_run):
    """Materialize CREATE OR REPLACE TABLE <table>_long for each detected pattern."""
    settings, project_dir, resolved_db_path = _resolve_tidy_settings(project_dir)
    if settings.database.mode != "duckdb":
        raise click.UsageError(
            "tidy table requires DuckDB; the apply path opens a writable DuckDB connection."
        )
    _, suggestions_by_table = asyncio.run(_gather_tidy_data(project_dir, source_table, settings))
    _apply_reshapes(suggestions_by_table, "table", dry_run, resolved_db_path)


tidy.add_command(tidy_suggest)
tidy.add_command(tidy_view)
tidy.add_command(tidy_table)
