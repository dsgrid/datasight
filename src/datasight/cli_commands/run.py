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

            datasight run
            datasight run --project-dir eia-demo
            datasight run --port 9000 --model gpt-4o
            datasight run --unix-socket /tmp/datasight.sock
        """
    )
)
@click.option("--port", type=int, default=None, help="Web UI port (default: 8084).")
@click.option("--host", default="127.0.0.1", help="Bind address for TCP mode.")
@click.option(
    "--unix-socket",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Listen on this UNIX domain socket instead of TCP.",
)
@click.option("--model", default=None, help="LLM model name (overrides .env).")
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=None,
    help="Auto-load this project on startup (optional).",
)
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def run(
    port,
    host,
    unix_socket,
    model,
    project_dir,
    verbose,
):
    """Start the datasight web UI.

    If the current directory contains schema_description.md, it will be
    auto-loaded as the project. Otherwise, use the UI to select a project,
    or pass --project-dir to specify one explicitly.
    """
    _, resolved_model, resolved_port = _prepare_web_runtime(
        port=port,
        model=model,
        project_dir=project_dir,
        verbose=verbose,
    )

    if project_dir:
        project_dir = str(Path(project_dir).resolve())

    click.echo(f"datasight v{cli_root.__version__}")
    click.echo(f"  Model:    {resolved_model}")
    if project_dir:
        click.echo(f"  Project:  {project_dir} (auto-load)")
    else:
        click.echo("  Project:  (none — select in UI)")
    if unix_socket:
        click.echo(f"  Socket:   {unix_socket}")
    else:
        click.echo(f"  Address:  http://{host}:{resolved_port}")
    click.echo()

    import uvicorn

    if unix_socket:
        if port is not None:
            raise click.UsageError("--port cannot be used with --unix-socket")
        os.environ["DATASIGHT_UNIX_SOCKET"] = str(unix_socket)
        click.echo(f"Starting web UI on UNIX socket {unix_socket} ...")
        uvicorn.run(
            "datasight.web.app:app",
            uds=str(unix_socket),
            log_level="warning",
        )
        return

    os.environ.pop("DATASIGHT_UNIX_SOCKET", None)
    click.echo(f"Starting web UI at http://{host}:{resolved_port} ...")
    uvicorn.run(
        "datasight.web.app:app",
        host=host,
        port=resolved_port,
        log_level="warning",
    )
