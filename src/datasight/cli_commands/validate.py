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

            datasight validate --scaffold
            datasight validate
            datasight validate --table generation_fuel
            datasight validate --format markdown -o validation.md
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Run rules for a specific table only.")
@click.option(
    "--config",
    "config_path",
    type=click.Path(),
    default=None,
    help="Path to validation.yaml (default: project_dir/validation.yaml).",
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
    help="Write the validation report to a file instead of stdout.",
)
@click.option(
    "--scaffold",
    is_flag=True,
    help="Write an example validation.yaml to the project directory and exit.",
)
@click.option("--overwrite", is_flag=True, help="Overwrite an existing validation.yaml.")
def validate(project_dir, table, config_path, output_format, output_path, scaffold, overwrite):
    """Run declarative validation rules against the database.

    Rules live in validation.yaml. Use --scaffold to create a starter file,
    edit it for your dataset, then run validate to produce pass/fail output.
    """
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())

    if scaffold:
        target = Path(config_path) if config_path else Path(project_dir) / "validation.yaml"
        if target.exists() and not overwrite:
            click.echo(
                f"Error: {target} already exists. Use --overwrite to replace.",
                err=True,
            )
            sys.exit(1)
        template = Path(cli_root.__file__).parent / "templates" / "validation.yaml"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(template.read_text(encoding="utf-8"), encoding="utf-8")
        click.echo(f"Wrote {target}. Edit the rules to match your dataset.")
        return

    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    rules = load_validation_config(config_path, project_dir)
    if not rules:
        click.echo(
            "No validation rules configured. Run `datasight validate --scaffold` "
            "to generate an example validation.yaml, then edit it for your dataset."
        )
        return

    if table:
        rules = [r for r in rules if r.get("table", "").lower() == table.lower()]
        if not rules:
            click.echo(f"No validation rules found for table: {table}")
            return

    async def _run_validate():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        return await build_validation_report(schema_info, sql_runner.run_sql, rules)

    validation_data = asyncio.run(_run_validate())

    if output_format == "json":
        _write_or_print(json.dumps(validation_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_validation_markdown(validation_data), output_path)
        return

    summary = validation_data.get("summary", {})
    console = Console(record=bool(output_path))
    console.print(
        _build_metric_table(
            "Validation Report",
            [
                ("Rules run", str(validation_data.get("rule_count", 0))),
                ("Pass", str(summary.get("pass", 0))),
                ("Fail", str(summary.get("fail", 0))),
                ("Warn", str(summary.get("warn", 0))),
            ],
        )
    )
    if validation_data["results"]:
        console.print(
            _build_profile_detail_table(
                "Results",
                [
                    ("Table", "left"),
                    ("Rule", "left"),
                    ("Column", "left"),
                    ("Status", "left"),
                    ("Detail", "left"),
                ],
                [
                    [
                        r["table"],
                        r["rule"],
                        r.get("column") or "-",
                        (
                            f"[green]{r['status'].upper()}[/green]"
                            if r["status"] == "pass"
                            else (
                                f"[red]{r['status'].upper()}[/red]"
                                if r["status"] == "fail"
                                else f"[yellow]{r['status'].upper()}[/yellow]"
                            )
                        ),
                        r["detail"],
                    ]
                    for r in validation_data["results"]
                ],
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)
