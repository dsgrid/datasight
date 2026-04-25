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

            datasight doctor
            datasight doctor --format markdown -o doctor.md
            datasight doctor --project-dir eia-demo
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
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
    help="Write doctor output to a file instead of stdout.",
)
def doctor(project_dir, output_format, output_path):
    """Check project configuration, local files, and database connectivity.

    Use this when a project will not load, an API key is missing, a database
    path is wrong, or the web UI cannot write state under .datasight/.
    """
    from rich.console import Console
    from rich.table import Table as RichTable

    project_path = Path(project_dir).resolve()
    console = Console(record=bool(output_path))
    checks: list[tuple[str, str, str]] = []

    def add_check(name: str, ok: bool, detail: str) -> None:
        checks.append((name, "OK" if ok else "FAIL", detail))

    env_path = project_path / ".env"
    add_check(".env", env_path.exists(), str(env_path))

    settings = _resolve_settings(str(project_path))[0]
    validation_errors = settings.validate()
    add_check(
        "LLM settings",
        not validation_errors,
        "; ".join(validation_errors) if validation_errors else settings.llm.provider,
    )

    db_detail = settings.database.mode
    db_ok = True
    resolved_db_path = _resolve_db_path(settings, str(project_path))
    if settings.database.mode in ("duckdb", "sqlite"):
        db_ok = bool(resolved_db_path) and os.path.exists(resolved_db_path)
        db_detail = resolved_db_path
    elif settings.database.mode == "postgres":
        db_ok = bool(
            settings.database.postgres_url
            or (
                settings.database.postgres_database
                and settings.database.postgres_user
                and settings.database.postgres_host
            )
        )
        db_detail = settings.database.postgres_url or (
            f"{settings.database.postgres_user}@{settings.database.postgres_host}:"
            f"{settings.database.postgres_port}/{settings.database.postgres_database}"
        )
    elif settings.database.mode == "flightsql":
        db_ok = bool(settings.database.flight_uri)
        db_detail = settings.database.flight_uri
    elif settings.database.mode == "spark":
        db_ok = bool(settings.database.spark_remote)
        db_detail = settings.database.spark_remote
    add_check("Database config", db_ok, db_detail or settings.database.mode)

    for name in ("schema_description.md", "queries.yaml"):
        path = project_path / name
        add_check(name, path.exists(), str(path))

    datasight_dir = project_path / ".datasight"
    try:
        datasight_dir.mkdir(parents=True, exist_ok=True)
        probe = datasight_dir / ".doctor-write-test"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        add_check(".datasight writable", True, str(datasight_dir))
    except OSError as exc:
        add_check(".datasight writable", False, f"{datasight_dir}: {exc}")

    try:
        sql_runner = create_sql_runner_from_settings(settings.database, str(project_path))
        asyncio.run(sql_runner.run_sql("SELECT 1 AS ok"))
        add_check("Database connectivity", True, "SELECT 1")
    except Exception as exc:
        add_check("Database connectivity", False, str(exc))

    rendered_checks = [
        {"name": name, "ok": status == "OK", "detail": detail} for name, status, detail in checks
    ]
    failures = sum(1 for check in rendered_checks if not check["ok"])

    if output_format == "json":
        _write_or_print(
            json.dumps(
                {
                    "project_dir": str(project_path),
                    "checks": rendered_checks,
                    "failures": failures,
                },
                indent=2,
            ),
            output_path,
        )
        if failures:
            sys.exit(1)
        return

    if output_format == "markdown":
        _write_or_print(
            _render_doctor_markdown(str(project_path), rendered_checks),
            output_path,
        )
        if failures:
            sys.exit(1)
        return

    table = RichTable(title="datasight doctor")
    table.add_column("Check")
    table.add_column("Status", no_wrap=True)
    table.add_column("Detail", overflow="fold")

    for name, status, detail in checks:
        if status == "FAIL":
            status_text = "[bold red]FAIL[/bold red]"
        else:
            status_text = "[green]OK[/green]"
        table.add_row(name, status_text, detail)

    console.print(table)
    if output_path:
        _write_or_print(console.export_text(), output_path)
    if failures:
        sys.exit(1)
