"""CLI command module."""

import asyncio
import json
import os
import sys
from pathlib import Path

import rich_click as click

from datasight.data_profile import (
    build_quality_overview,
    find_table_info,
)

from datasight import cli
from datasight.cli_helpers import format_epilog


@click.command(
    epilog=format_epilog(
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
    "--deep",
    is_flag=True,
    default=False,
    help=(
        "Run expensive detectors: whole-row and PK-shaped duplicates, text "
        "whitespace/empty-string flags, IQR-based numeric outliers, and "
        "orphan foreign-key-shaped values."
    ),
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
    help="Write the quality audit to a file instead of stdout.",
)
def quality(project_dir, table, deep, output_format, output_path):  # noqa: C901
    """Audit data quality - nulls, suspicious ranges, and date coverage.

    Also checks temporal completeness when time_series.yaml defines expected
    time series structure.
    """
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    settings, _ = cli.resolve_settings(project_dir)
    resolved_db_path = cli.resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    from datasight.config import load_time_series_config
    from datasight.data_profile import build_time_series_quality
    from datasight.tidy import analyze_tidy_patterns

    time_series_configs = load_time_series_config(None, project_dir)

    async def _run_quality():
        sql_runner, schema_info = await cli.load_schema_info_for_project(project_dir, settings)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                msg = f"Table not found: {table}"
                raise click.ClickException(msg)
            schema_info = [table_info]
        base = await build_quality_overview(
            schema_info,
            sql_runner.run_sql,
            sql_dialect=settings.database.sql_dialect,
            deep=deep,
        )
        ts_configs = time_series_configs
        if table and ts_configs:
            ts_configs = [c for c in ts_configs if c["table"].lower() == table.lower()]
        if ts_configs:
            ts_data = await build_time_series_quality(ts_configs, sql_runner.run_sql)
            base["time_series_issues"] = ts_data.get("time_series_issues", [])
            base["time_series_summaries"] = ts_data.get("time_series_summaries", [])
        tidy = analyze_tidy_patterns(schema_info)
        base["tidy_suggestions"] = tidy["suggestions"]
        base["wide_tables"] = tidy["wide_tables"]
        return base

    quality_data = asyncio.run(_run_quality())

    if output_format == "json":
        cli.write_or_print(json.dumps(quality_data, indent=2), output_path)
        return

    if output_format == "markdown":
        cli.write_or_print(cli.render_quality_markdown(quality_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        cli.build_metric_table(
            "Dataset Quality Audit",
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
    if quality_data["date_columns"]:
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
                    for item in quality_data["date_columns"]
                ],
            )
        )
    if quality_data.get("time_series_summaries"):
        console.print(
            cli.build_profile_detail_table(
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
            cli.build_profile_detail_table(
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
    if quality_data.get("tidy_suggestions"):
        console.print(
            cli.build_profile_detail_table(
                "Tidy Reshape Suggestions",
                [
                    ("Table", "left"),
                    ("Pattern", "left"),
                    ("Dimensions", "left"),
                    ("Columns", "right"),
                    ("Rationale", "left"),
                ],
                [
                    [
                        item["table"],
                        item["pattern"],
                        ", ".join(d["name"] for d in item["dimensions"]),
                        str(len(item["column_mappings"])),
                        item["rationale"],
                    ]
                    for item in quality_data["tidy_suggestions"]
                ],
            )
        )
    if quality_data.get("wide_tables"):
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
                    for item in quality_data["wide_tables"]
                ],
            )
        )
    cleanup_blocks: list[tuple[str, str]] = []
    if quality_data.get("duplicate_rows"):
        console.print(
            cli.build_profile_detail_table(
                "Whole-Row Duplicates",
                [("Table", "left"), ("Duplicate rows", "right")],
                [
                    [item["table"], str(item["duplicate_count"])]
                    for item in quality_data["duplicate_rows"]
                ],
            )
        )
        for item in quality_data["duplicate_rows"]:
            if item.get("cleanup_sql"):
                cleanup_blocks.append((f"{item['table']} (whole-row dedup)", item["cleanup_sql"]))
    if quality_data.get("pk_duplicates"):
        console.print(
            cli.build_profile_detail_table(
                "Primary-Key-Shaped Duplicates",
                [("Column", "left"), ("Sample duplicates", "left")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        ", ".join(f"{e['value']} (×{e['count']})" for e in item["examples"][:3]),
                    ]
                    for item in quality_data["pk_duplicates"]
                ],
            )
        )
        for item in quality_data["pk_duplicates"]:
            if item.get("cleanup_sql"):
                cleanup_blocks.append(
                    (f"{item['table']}.{item['column']} (PK dedup)", item["cleanup_sql"])
                )
    if quality_data.get("text_flags"):
        console.print(
            cli.build_profile_detail_table(
                "Text Cleanliness",
                [("Column", "left"), ("Issue", "left"), ("Rows", "right")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        item["issue"],
                        str(item["count"]),
                    ]
                    for item in quality_data["text_flags"]
                ],
            )
        )
        for item in quality_data["text_flags"]:
            if item.get("cleanup_sql"):
                cleanup_blocks.append(
                    (
                        f"{item['table']}.{item['column']} ({item['issue']})",
                        item["cleanup_sql"],
                    )
                )
    if quality_data.get("outlier_flags"):
        console.print(
            cli.build_profile_detail_table(
                "Numeric Outliers (IQR)",
                [("Column", "left"), ("Rows", "right"), ("Q1", "right"), ("Q3", "right")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        str(item["outlier_count"]),
                        cli.format_profile_value(item.get("q1")),
                        cli.format_profile_value(item.get("q3")),
                    ]
                    for item in quality_data["outlier_flags"]
                ],
            )
        )
        for item in quality_data["outlier_flags"]:
            if item.get("cleanup_sql"):
                cleanup_blocks.append(
                    (f"{item['table']}.{item['column']} (outliers)", item["cleanup_sql"])
                )
    if quality_data.get("orphan_flags"):
        console.print(
            cli.build_profile_detail_table(
                "Orphan Foreign-Key-Shaped Values",
                [("Child", "left"), ("Parent", "left"), ("Orphans", "right")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        f"{item['parent_table']}.{item['parent_column']}",
                        str(item["orphan_count"]),
                    ]
                    for item in quality_data["orphan_flags"]
                ],
            )
        )
        for item in quality_data["orphan_flags"]:
            if item.get("cleanup_sql"):
                cleanup_blocks.append(
                    (
                        f"{item['table']}.{item['column']} (orphans → {item['parent_table']})",
                        item["cleanup_sql"],
                    )
                )
    if cleanup_blocks:
        from rich.panel import Panel
        from rich.syntax import Syntax

        console.print()
        console.print("[bold]Suggested Cleanup[/bold]")
        for title, sql in cleanup_blocks:
            console.print(
                Panel(
                    Syntax(sql, "sql", theme="ansi_dark", word_wrap=True),
                    title=title,
                    border_style="dim",
                )
            )
    if quality_data["notes"]:
        console.print(
            cli.build_profile_detail_table(
                "Notes",
                [("Observation", "left")],
                [[item] for item in quality_data["notes"]],
            )
        )
    if output_path:
        cli.write_or_print(console.export_text(), output_path)
