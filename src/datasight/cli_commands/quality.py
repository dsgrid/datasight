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

            datasight quality
            datasight quality --table generation_fuel
            datasight quality --format markdown -o quality.md
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Audit a specific table.")
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
    help="Write the quality audit to a file instead of stdout.",
)
def quality(project_dir, table, output_format, output_path):
    """Audit data quality - nulls, suspicious ranges, and date coverage.

    Also checks temporal completeness when time_series.yaml defines expected
    time series structure.
    """
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    from datasight.config import load_time_series_config
    from datasight.data_profile import build_time_series_quality

    time_series_configs = load_time_series_config(None, project_dir)

    async def _run_quality():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info = [table_info]
        base = await build_quality_overview(schema_info, sql_runner.run_sql)
        ts_configs = time_series_configs
        if table and ts_configs:
            ts_configs = [c for c in ts_configs if c["table"].lower() == table.lower()]
        if ts_configs:
            ts_data = await build_time_series_quality(ts_configs, sql_runner.run_sql)
            base["time_series_issues"] = ts_data.get("time_series_issues", [])
            base["time_series_summaries"] = ts_data.get("time_series_summaries", [])
        return base

    quality_data = asyncio.run(_run_quality())

    if output_format == "json":
        _write_or_print(json.dumps(quality_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_quality_markdown(quality_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        _build_metric_table(
            "Dataset Quality Audit",
            [("Tables scanned", str(quality_data["table_count"]))],
        )
    )
    if quality_data["null_columns"]:
        console.print(
            _build_profile_detail_table(
                "Null-heavy Columns",
                [("Column", "left"), ("Nulls", "right"), ("Null %", "right")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        str(item["null_count"]),
                        str(item.get("null_rate") or 0),
                    ]
                    for item in quality_data["null_columns"]
                ],
            )
        )
    if quality_data["numeric_flags"]:
        console.print(
            _build_profile_detail_table(
                "Numeric Range Flags",
                [("Column", "left"), ("Issue", "left")],
                [
                    [f"{item['table']}.{item['column']}", item["issue"]]
                    for item in quality_data["numeric_flags"]
                ],
            )
        )
    if quality_data["date_columns"]:
        console.print(
            _build_profile_detail_table(
                "Date Coverage",
                [("Column", "left"), ("Min", "left"), ("Max", "left")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        _format_profile_value(item.get("min")),
                        _format_profile_value(item.get("max")),
                    ]
                    for item in quality_data["date_columns"]
                ],
            )
        )
    if quality_data.get("time_series_summaries"):
        console.print(
            _build_profile_detail_table(
                "Time Series",
                [("Column", "left"), ("Frequency", "left"), ("Rows", "right"), ("Range", "left")],
                [
                    [
                        f"{s['table']}.{s['timestamp_column']}",
                        s.get("frequency", ""),
                        str(s.get("total_rows", "")),
                        f"{s.get('min_ts', '')} — {s.get('max_ts', '')}",
                    ]
                    for s in quality_data["time_series_summaries"]
                ],
            )
        )
    if quality_data.get("time_series_issues"):
        console.print(
            _build_profile_detail_table(
                "Temporal Completeness",
                [("Column", "left"), ("Issue", "left"), ("Detail", "left")],
                [
                    [
                        f"{item['table']}.{item['timestamp_column']}",
                        item["issue"],
                        item["detail"],
                    ]
                    for item in quality_data["time_series_issues"]
                ],
            )
        )
    if quality_data["notes"]:
        console.print(
            _build_profile_detail_table(
                "Notes",
                [("Observation", "left")],
                [[item] for item in quality_data["notes"]],
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)
