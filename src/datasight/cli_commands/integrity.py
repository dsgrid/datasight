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

            datasight integrity
            datasight integrity --table plants
            datasight integrity --format json -o integrity.json
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Focus integrity checks on a specific table.")
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
    help="Write the integrity audit to a file instead of stdout.",
)
def integrity(project_dir, table, output_format, output_path):
    """Audit cross-table referential integrity - keys, orphans, and join risks.

    Use this to find likely primary keys, duplicate keys, orphaned foreign
    keys, and joins that may multiply rows unexpectedly.
    """
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    from datasight.config import load_joins_config

    declared_joins = load_joins_config(None, project_dir) or None

    async def _run_integrity():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info_filtered = [table_info]
        else:
            schema_info_filtered = schema_info
        return await build_integrity_overview(
            schema_info_filtered, sql_runner.run_sql, declared_joins
        )

    integrity_data = asyncio.run(_run_integrity())

    if output_format == "json":
        _write_or_print(json.dumps(integrity_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_integrity_markdown(integrity_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        _build_metric_table(
            "Referential Integrity",
            [("Tables scanned", str(integrity_data["table_count"]))],
        )
    )
    if integrity_data["primary_keys"]:
        console.print(
            _build_profile_detail_table(
                "Primary Keys",
                [
                    ("Table", "left"),
                    ("Column", "left"),
                    ("Distinct", "right"),
                    ("Rows", "right"),
                    ("Unique", "left"),
                ],
                [
                    [
                        item["table"],
                        item["column"],
                        str(item["distinct_count"]),
                        str(item["row_count"]),
                        "yes" if item["is_unique"] else "NO",
                    ]
                    for item in integrity_data["primary_keys"]
                ],
            )
        )
    if integrity_data["duplicate_keys"]:
        console.print(
            _build_profile_detail_table(
                "Duplicate Keys",
                [("Table", "left"), ("Column", "left"), ("Duplicates", "right")],
                [
                    [item["table"], item["column"], str(item["duplicate_count"])]
                    for item in integrity_data["duplicate_keys"]
                ],
            )
        )
    if integrity_data["orphan_foreign_keys"]:
        console.print(
            _build_profile_detail_table(
                "Orphan Foreign Keys",
                [
                    ("Child", "left"),
                    ("Parent", "left"),
                    ("Orphans", "right"),
                    ("Child Rows", "right"),
                ],
                [
                    [
                        f"{item['child_table']}.{item['child_column']}",
                        f"{item['parent_table']}.{item['parent_column']}",
                        str(item["orphan_count"]),
                        str(item["child_rows"]),
                    ]
                    for item in integrity_data["orphan_foreign_keys"]
                ],
            )
        )
    if integrity_data["join_explosions"]:
        console.print(
            _build_profile_detail_table(
                "Join Explosion Risks",
                [
                    ("Table A", "left"),
                    ("Table B", "left"),
                    ("Column", "left"),
                    ("Expected", "right"),
                    ("Actual", "right"),
                    ("Factor", "right"),
                ],
                [
                    [
                        item["table_a"],
                        item["table_b"],
                        item["join_column"],
                        str(item["expected_rows"]),
                        str(item["actual_rows"]),
                        f"{item['explosion_factor']}x",
                    ]
                    for item in integrity_data["join_explosions"]
                ],
            )
        )
    if integrity_data["notes"]:
        console.print(
            _build_profile_detail_table(
                "Notes",
                [("Observation", "left")],
                [[item] for item in integrity_data["notes"]],
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)
