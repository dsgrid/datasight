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
    epilog=_epilog(
        """
        Examples:

            datasight dimensions
            datasight dimensions --table generation_fuel
            datasight dimensions --format json -o dimensions.json
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Inspect dimensions for a specific table.")
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
    help="Write the dimension overview to a file instead of stdout.",
)
def dimensions(project_dir, table, output_format, output_path):
    """Surface likely grouping dimensions and category breakdowns.

    Use this to find text/code columns that are good GROUP BY candidates,
    such as fuel codes, states, sectors, plants, or scenario labels.
    """
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    async def _run_dimensions():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info = [table_info]
        return await build_dimension_overview(schema_info, sql_runner.run_sql)

    dimension_data = asyncio.run(_run_dimensions())

    if output_format == "json":
        _write_or_print(json.dumps(dimension_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_dimensions_markdown(dimension_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        _build_metric_table(
            "Dimension Overview",
            [("Tables scanned", str(dimension_data["table_count"]))],
        )
    )
    if dimension_data["dimension_columns"]:
        console.print(
            _build_profile_detail_table(
                "Dimension Candidates",
                [
                    ("Column", "left"),
                    ("Distinct", "right"),
                    ("Null %", "right"),
                    ("Samples", "left"),
                ],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        _format_profile_value(item.get("distinct_count")),
                        _format_profile_value(item.get("null_rate"), "0"),
                        ", ".join((item.get("sample_values") or [])[:3]) or "none",
                    ]
                    for item in dimension_data["dimension_columns"]
                ],
            )
        )
    if dimension_data["suggested_breakdowns"]:
        console.print(
            _build_profile_detail_table(
                "Suggested Breakdowns",
                [("Column", "left"), ("Reason", "left")],
                [
                    [f"{item['table']}.{item['column']}", item["reason"]]
                    for item in dimension_data["suggested_breakdowns"]
                ],
            )
        )
    if dimension_data["join_hints"]:
        console.print(
            _build_profile_detail_table(
                "Join Hints", [("Hint", "left")], [[item] for item in dimension_data["join_hints"]]
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)
