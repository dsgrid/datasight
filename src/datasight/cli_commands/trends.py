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

            datasight trends
            datasight trends --table generation_fuel
            datasight trends generation.parquet plants.parquet
            datasight trends --format markdown -o trends.md
        """
    )
)
@click.argument("files", nargs=-1, type=click.Path(exists=True))
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=None,
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Suggest trends for a specific table.")
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
    help="Write the trend overview to a file instead of stdout.",
)
def trends(files, project_dir, table, output_format, output_path):
    """Surface likely trend analyses and chart recommendations.

    Run inside a configured project, or pass one or more Parquet, CSV, Excel,
    or DuckDB files directly for a quick file-only trend scan.
    """
    from rich.console import Console
    from datasight.config import load_measure_overrides

    async def _run_trends():
        if files:
            from datasight.explore import create_files_session_for_settings
            from datasight.schema import introspect_schema

            db_settings = _current_db_settings_or_none()
            runner, _ = create_files_session_for_settings(list(files), db_settings)
            tables = await introspect_schema(runner.run_sql, runner=runner)
            schema_info = [
                {
                    "name": t.name,
                    "row_count": t.row_count,
                    "columns": [
                        {"name": c.name, "dtype": c.dtype, "nullable": c.nullable}
                        for c in t.columns
                    ],
                }
                for t in tables
            ]
            sql_runner = runner
            measure_overrides: list[dict[str, Any]] = []
        else:
            resolved_dir = str(Path(project_dir or ".").resolve())
            settings, _ = _resolve_settings(resolved_dir)
            resolved_db_path = _resolve_db_path(settings, resolved_dir)
            if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(
                resolved_db_path
            ):
                click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
                sys.exit(1)
            sql_runner, schema_info = await _load_schema_info_for_project(resolved_dir, settings)
            measure_overrides = load_measure_overrides(None, resolved_dir)

        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info = [table_info]
        return await build_trend_overview(schema_info, sql_runner.run_sql, measure_overrides)

    trend_data = asyncio.run(_run_trends())

    if output_format == "json":
        _write_or_print(json.dumps(trend_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_trends_markdown(trend_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        _build_metric_table(
            "Trend Overview",
            [("Tables scanned", str(trend_data["table_count"]))],
        )
    )
    if trend_data["trend_candidates"]:
        console.print(
            _build_profile_detail_table(
                "Trend Candidates",
                [
                    ("Table", "left"),
                    ("Date", "left"),
                    ("Aggregation", "left"),
                    ("Measure", "left"),
                    ("Range", "left"),
                ],
                [
                    [
                        item["table"],
                        item["date_column"],
                        str(item.get("aggregation") or "").upper(),
                        item["measure_column"],
                        item["date_range"],
                    ]
                    for item in trend_data["trend_candidates"]
                ],
            )
        )
    if trend_data["breakout_dimensions"]:
        console.print(
            _build_profile_detail_table(
                "Breakout Dimensions",
                [("Column", "left"), ("Distinct", "right"), ("Null %", "right")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        _format_profile_value(item.get("distinct_count")),
                        _format_profile_value(item.get("null_rate"), "0"),
                    ]
                    for item in trend_data["breakout_dimensions"]
                ],
            )
        )
    if trend_data["chart_recommendations"]:
        console.print(
            _build_profile_detail_table(
                "Chart Recommendations",
                [("Title", "left"), ("Type", "left"), ("Reason", "left")],
                [
                    [item["title"], item["chart_type"], item["reason"]]
                    for item in trend_data["chart_recommendations"]
                ],
            )
        )
    if trend_data["notes"]:
        console.print(
            _build_profile_detail_table(
                "Notes", [("Observation", "left")], [[item] for item in trend_data["notes"]]
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)
