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

            datasight distribution
            datasight distribution --table generation_fuel
            datasight distribution --column generation_fuel.net_generation_mwh
            datasight distribution --format markdown -o distributions.md
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Profile distributions for a specific table.")
@click.option(
    "--column",
    default=None,
    help="Focus on a specific column as table.column.",
)
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
    help="Write the distribution profile to a file instead of stdout.",
)
def distribution(project_dir, table, column, output_format, output_path):
    """Profile value distributions - percentiles, outliers, and measure flags.

    Use this to inspect numeric ranges, skew, zero/negative rates, outliers,
    and measure-semantic flags before building charts or validation rules.
    """
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    if table and column:
        click.echo("Error: use either --table or --column, not both.", err=True)
        sys.exit(1)

    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    from datasight.config import load_measure_overrides

    measure_overrides = load_measure_overrides(None, project_dir)

    async def _run_distribution():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info_filtered = [table_info]
        else:
            schema_info_filtered = schema_info
        return await build_distribution_overview(
            schema_info_filtered, sql_runner.run_sql, measure_overrides, column
        )

    dist_data = asyncio.run(_run_distribution())

    if output_format == "json":
        _write_or_print(json.dumps(dist_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_distribution_markdown(dist_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        _build_metric_table(
            "Distribution Profiling",
            [("Tables scanned", str(dist_data["table_count"]))],
        )
    )
    if dist_data["distributions"]:
        console.print(
            _build_profile_detail_table(
                "Distributions",
                [
                    ("Column", "left"),
                    ("p5", "right"),
                    ("p50", "right"),
                    ("p95", "right"),
                    ("Zero %", "right"),
                    ("Neg %", "right"),
                    ("Outliers", "right"),
                ],
                [
                    [
                        f"{d['table']}.{d['column']}",
                        _fmt_dist(d.get("p5")),
                        _fmt_dist(d.get("p50")),
                        _fmt_dist(d.get("p95")),
                        _fmt_dist(d.get("zero_rate")),
                        _fmt_dist(d.get("negative_rate")),
                        str(d.get("outlier_count", 0)),
                    ]
                    for d in dist_data["distributions"]
                ],
            )
        )
    if dist_data["energy_flags"]:
        console.print(
            _build_profile_detail_table(
                "Energy Flags",
                [("Column", "left"), ("Flag", "left"), ("Detail", "left")],
                [
                    [f"{f['table']}.{f['column']}", f["flag"], f["detail"]]
                    for f in dist_data["energy_flags"]
                ],
            )
        )
    if dist_data["spikes"]:
        console.print(
            _build_profile_detail_table(
                "Temporal Spikes",
                [("Column", "left"), ("Period", "left"), ("Z-score", "right"), ("Detail", "left")],
                [
                    [
                        f"{s['table']}.{s['measure_column']}",
                        s["period"],
                        str(s["z_score"]),
                        s["detail"],
                    ]
                    for s in dist_data["spikes"]
                ],
            )
        )
    if dist_data["notes"]:
        console.print(
            _build_profile_detail_table(
                "Notes",
                [("Observation", "left")],
                [[item] for item in dist_data["notes"]],
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)
