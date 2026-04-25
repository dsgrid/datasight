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

            datasight measures
            datasight measures --table generation_fuel
            datasight measures --scaffold
            datasight measures --format markdown -o measures.md
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Inspect measures for a specific table.")
@click.option(
    "--scaffold", is_flag=True, help="Write an editable measures.yaml scaffold and exit."
)
@click.option("--overwrite", is_flag=True, help="Overwrite an existing scaffold file.")
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
    help="Write the measure overview to a file instead of stdout.",
)
def measures(project_dir, table, scaffold, overwrite, output_format, output_path):
    """Surface likely measures and default aggregations.

    Measures are numeric columns that should usually be summed, averaged,
    or otherwise aggregated in generated SQL. Use --scaffold to create an
    editable measures.yaml override file.
    """
    from rich.console import Console
    from datasight.config import load_measure_overrides

    project_dir = str(Path(project_dir).resolve())
    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    async def _run_measures():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        measure_overrides = load_measure_overrides(None, project_dir)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info = [table_info]
        return await build_measure_overview(schema_info, sql_runner.run_sql, measure_overrides)

    measure_data = asyncio.run(_run_measures())

    if scaffold:
        scaffold_path = Path(output_path) if output_path else Path(project_dir) / "measures.yaml"
        if scaffold_path.exists() and not overwrite:
            click.echo(
                f"Error: {scaffold_path} already exists. Use --overwrite to replace.",
                err=True,
            )
            sys.exit(1)
        scaffold_path.parent.mkdir(parents=True, exist_ok=True)
        scaffold_path.write_text(
            format_measure_overrides_yaml(measure_data),
            encoding="utf-8",
        )
        click.echo(f"Measure override scaffold saved to {scaffold_path}")
        return

    if output_format == "json":
        _write_or_print(json.dumps(measure_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_measures_markdown(measure_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        _build_metric_table(
            "Measure Overview",
            [("Tables scanned", str(measure_data["table_count"]))],
        )
    )
    if measure_data["measures"]:
        console.print(
            _build_profile_detail_table(
                "Measure Candidates",
                [
                    ("Column", "left"),
                    ("Role", "left"),
                    ("Unit", "left"),
                    ("Default", "left"),
                    ("Averaging", "left"),
                    ("Rollup SQL", "left"),
                    ("Allowed", "left"),
                    ("Additive", "left"),
                ],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        item["role"]
                        + (f" [{item['display_name']}]" if item.get("display_name") else ""),
                        _format_profile_value(item.get("unit"), "—"),
                        item["default_aggregation"]
                        + (f" ({item['format']})" if item.get("format") else ""),
                        (
                            f"weighted by {item['weight_column']}"
                            if item.get("weight_column")
                            else item.get("average_strategy", "avg")
                        ),
                        item["recommended_rollup_sql"],
                        (
                            (", ".join(item["allowed_aggregations"]))
                            + (f" | expr: {item['expression']}" if item.get("expression") else "")
                            + (
                                f" | charts: {', '.join(item['preferred_chart_types'])}"
                                if item.get("preferred_chart_types")
                                else ""
                            )
                        ),
                        (
                            ("category" if item.get("additive_across_category") else "")
                            + (
                                ", time"
                                if item.get("additive_across_category")
                                and item.get("additive_across_time")
                                else ("time" if item.get("additive_across_time") else "")
                            )
                        )
                        or "no",
                    ]
                    for item in measure_data["measures"]
                ],
            )
        )
        console.print(
            _build_profile_detail_table(
                "Aggregation Guidance",
                [("Column", "left"), ("Avoid", "left"), ("Why", "left")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        ", ".join(item.get("forbidden_aggregations") or []) or "—",
                        item["reason"],
                    ]
                    for item in measure_data["measures"]
                ],
            )
        )
    if measure_data["notes"]:
        console.print(
            _build_profile_detail_table(
                "Notes", [("Observation", "left")], [[item] for item in measure_data["notes"]]
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)
