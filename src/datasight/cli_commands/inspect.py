"""CLI command module."""

import asyncio
import json

import rich_click as click

from datasight.data_profile import (
    build_dataset_overview,
    build_dimension_overview,
    build_measure_overview,
    build_prompt_recipes,
    build_quality_overview,
    build_trend_overview,
)

from datasight import cli
from datasight.cli_helpers import format_epilog


@click.command(
    epilog=format_epilog(
        """
        Examples:

            datasight inspect generation.parquet
            datasight inspect generation.csv plants.csv
            datasight inspect data_dir/
            datasight inspect generation.parquet --format markdown -o inspect.md
        """
    )
)
@click.argument("files", nargs=-1, required=True, type=click.Path(exists=True))
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
    help="Write the full report to a file instead of stdout.",
)
def inspect(files, output_format, output_path):  # noqa: C901
    """Run all analyses on Parquet, CSV, Excel, or DuckDB files and print results.

    Creates a file-backed session and runs profile, quality, measures,
    dimensions, trends, and recipes — printing everything to the console
    without creating a project. When the current directory contains a
    ``.env`` with ``DB_MODE=spark``, the files are registered as Spark
    temp views and all queries run on the cluster; otherwise an ephemeral
    in-memory DuckDB session is used.
    """
    import time as _time

    from loguru import logger as _logger
    from rich.console import Console

    from datasight.explore import create_files_session_for_settings
    from datasight.schema import introspect_schema

    db_settings = cli.current_db_settings_or_none()

    async def _run_phase(name: str, coro):
        _logger.info(f"[inspect] {name}…")
        t0 = _time.perf_counter()
        result = await coro
        _logger.info(f"[inspect] {name} done in {_time.perf_counter() - t0:.1f}s")
        return result

    async def _run_all():
        runner, tables_info = create_files_session_for_settings(list(files), db_settings)
        tables = await _run_phase(
            f"introspecting schema for {len(tables_info)} table(s)",
            introspect_schema(runner.run_sql, runner=runner),
        )
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

        profile_data = await _run_phase(
            "profiling tables", build_dataset_overview(schema_info, runner.run_sql)
        )
        quality_data = await _run_phase(
            "running quality checks",
            build_quality_overview(
                schema_info,
                runner.run_sql,
                sql_dialect=(db_settings.sql_dialect if db_settings else "duckdb"),
            ),
        )
        measure_data = await _run_phase(
            "discovering measures",
            build_measure_overview(schema_info, runner.run_sql, overrides=None),
        )
        dimension_data = await _run_phase(
            "discovering dimensions", build_dimension_overview(schema_info, runner.run_sql)
        )
        trend_data = await _run_phase(
            "scanning for trends",
            build_trend_overview(schema_info, runner.run_sql, overrides=None),
        )
        recipe_list = await _run_phase(
            "building prompt recipes",
            build_prompt_recipes(schema_info, runner.run_sql, overrides=None),
        )
        recipes_data = [{"id": idx, **r} for idx, r in enumerate(recipe_list, start=1)]

        return {
            "files": [t["name"] for t in tables_info],
            "profile": profile_data,
            "quality": quality_data,
            "measures": measure_data,
            "dimensions": dimension_data,
            "trends": trend_data,
            "recipes": recipes_data,
        }

    results = asyncio.run(_run_all())

    if output_format == "json":
        cli.write_or_print(json.dumps(results, indent=2), output_path)
        return

    if output_format == "markdown":
        sections = [
            cli.render_profile_markdown("dataset", results["profile"]),
            cli.render_quality_markdown(results["quality"]),
            cli.render_measures_markdown(results["measures"]),
            cli.render_dimensions_markdown(results["dimensions"]),
            cli.render_trends_markdown(results["trends"]),
            cli.render_recipes_markdown(results["recipes"]),
        ]
        cli.write_or_print("\n\n".join(sections), output_path)
        return

    console = Console(record=bool(output_path))
    file_label = ", ".join(str(f) for f in files)
    console.print(f"\n[bold]datasight inspect:[/bold] {file_label}\n")

    # --- Profile ---
    profile_data = results["profile"]
    console.print(
        cli.build_metric_table(
            "Dataset Profile",
            [
                ("Tables", str(profile_data["table_count"])),
                ("Columns", str(profile_data["total_columns"])),
                ("Rows", str(profile_data["total_rows"])),
            ],
        )
    )
    if profile_data["largest_tables"]:
        console.print(
            cli.build_profile_detail_table(
                "Largest Tables",
                [("Table", "left"), ("Rows", "right"), ("Columns", "right")],
                [
                    [
                        item["name"],
                        f"{item.get('row_count') or 0}",
                        str(item["column_count"]),
                    ]
                    for item in profile_data["largest_tables"]
                ],
            )
        )
    if profile_data["date_columns"]:
        console.print(
            cli.build_profile_detail_table(
                "Date Coverage",
                [("Column", "left"), ("Min", "left"), ("Max", "left")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        cli.format_profile_value(item.get("min")),
                        cli.format_profile_value(item.get("max")),
                    ]
                    for item in profile_data["date_columns"]
                ],
            )
        )

    # --- Quality ---
    quality_data = results["quality"]
    if quality_data["null_columns"] or quality_data["numeric_flags"]:
        console.print(
            cli.build_metric_table(
                "Quality Audit",
                [("Tables scanned", str(quality_data["table_count"]))],
            )
        )
        if quality_data["null_columns"]:
            console.print(
                cli.build_profile_detail_table(
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
                cli.build_profile_detail_table(
                    "Numeric Range Flags",
                    [("Column", "left"), ("Issue", "left")],
                    [
                        [f"{item['table']}.{item['column']}", item["issue"]]
                        for item in quality_data["numeric_flags"]
                    ],
                )
            )
        if quality_data["notes"]:
            console.print(
                cli.build_profile_detail_table(
                    "Quality Notes",
                    [("Observation", "left")],
                    [[item] for item in quality_data["notes"]],
                )
            )

    # --- Measures ---
    measure_data = results["measures"]
    if measure_data["measures"]:
        console.print(
            cli.build_profile_detail_table(
                "Measure Candidates",
                [
                    ("Column", "left"),
                    ("Role", "left"),
                    ("Unit", "left"),
                    ("Default Agg", "left"),
                    ("Rollup SQL", "left"),
                ],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        item["role"]
                        + (f" [{item['display_name']}]" if item.get("display_name") else ""),
                        cli.format_profile_value(item.get("unit"), "—"),
                        item["default_aggregation"],
                        item["recommended_rollup_sql"],
                    ]
                    for item in measure_data["measures"]
                ],
            )
        )

    # --- Dimensions ---
    dimension_data = results["dimensions"]
    if dimension_data["dimension_columns"]:
        console.print(
            cli.build_profile_detail_table(
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
                        cli.format_profile_value(item.get("distinct_count")),
                        cli.format_profile_value(item.get("null_rate"), "0"),
                        ", ".join((item.get("sample_values") or [])[:3]) or "none",
                    ]
                    for item in dimension_data["dimension_columns"]
                ],
            )
        )
    if dimension_data["suggested_breakdowns"]:
        console.print(
            cli.build_profile_detail_table(
                "Suggested Breakdowns",
                [("Column", "left"), ("Reason", "left")],
                [
                    [f"{item['table']}.{item['column']}", item["reason"]]
                    for item in dimension_data["suggested_breakdowns"]
                ],
            )
        )

    # --- Trends ---
    trend_data = results["trends"]
    if trend_data["trend_candidates"]:
        console.print(
            cli.build_profile_detail_table(
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
    if trend_data["chart_recommendations"]:
        console.print(
            cli.build_profile_detail_table(
                "Chart Recommendations",
                [("Title", "left"), ("Type", "left"), ("Reason", "left")],
                [
                    [item["title"], item["chart_type"], item["reason"]]
                    for item in trend_data["chart_recommendations"]
                ],
            )
        )

    # --- Recipes ---
    recipes_data = results["recipes"]
    if recipes_data:
        console.print(
            cli.build_profile_detail_table(
                "Prompt Recipes",
                [
                    ("ID", "right"),
                    ("Title", "left"),
                    ("Category", "left"),
                    ("Why", "left"),
                    ("Prompt", "left"),
                ],
                [
                    [
                        str(item["id"]),
                        item["title"],
                        item.get("category", ""),
                        item.get("reason", ""),
                        item["prompt"][:80] + ("…" if len(item["prompt"]) > 80 else ""),
                    ]
                    for item in recipes_data
                ],
            )
        )

    if output_path:
        cli.write_or_print(console.export_text(), output_path)
