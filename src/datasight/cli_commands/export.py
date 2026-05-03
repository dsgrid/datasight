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

            datasight export --list-sessions
            datasight export abc123def -o my-analysis.html
            datasight export abc123def --format py -o my-analysis.py
            datasight export abc123def --exclude 2,3
        """
    )
)
@click.argument("session_id", required=False)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help=(
        "Output file path. Defaults to <session_id>.<format> with the "
        "session ID truncated to 20 characters."
    ),
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["html", "py"], case_sensitive=False),
    default="html",
    help="html (self-contained viewer, default) or py (runnable Python script).",
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .datasight/conversations/.",
)
@click.option(
    "--exclude",
    default=None,
    help="Comma-separated turn indices to exclude (0-based, each turn is a Q&A pair).",
)
@click.option("--list-sessions", is_flag=True, help="List available sessions and exit.")
def export(session_id, output_path, output_format, project_dir, exclude, list_sessions):
    """Export a conversation session as a self-contained HTML page or Python script.

    SESSION_ID is the conversation ID (use --list-sessions to see available IDs).
    """
    import json as json_mod

    project_dir = str(Path(project_dir).resolve())
    conv_dir = Path(project_dir) / ".datasight" / "conversations"

    if list_sessions or session_id == "list":
        if not conv_dir.exists():
            click.echo("No conversations found.")
            return
        sessions = []
        for f in sorted(conv_dir.glob("*.json")):
            try:
                data = json_mod.loads(f.read_text(encoding="utf-8"))
                events = data.get("events", [])
                msg_count = sum(1 for e in events if e.get("event") == "user_message")
                if msg_count == 0:
                    continue
                sessions.append(
                    {
                        "id": f.stem,
                        "title": data.get("title", "Untitled"),
                        "messages": msg_count,
                    }
                )
            except (json_mod.JSONDecodeError, OSError):
                continue
        if not sessions:
            click.echo("No conversations found.")
            return

        from rich.console import Console
        from rich.table import Table

        console = Console()
        table = Table(title="Available Sessions")
        table.add_column("Session ID", style="cyan", no_wrap=True)
        table.add_column("Title", overflow="fold")
        table.add_column("Messages", justify="right")
        for s in sessions:
            table.add_row(s["id"], s["title"], str(s["messages"]))
        console.print(table)
        return

    if not session_id:
        click.echo(
            "Error: provide a SESSION_ID or use --list-sessions to see available sessions.",
            err=True,
        )
        sys.exit(1)

    # Load session
    session_path = conv_dir / f"{session_id}.json"
    if not session_path.exists():
        click.echo(f"Error: Session not found: {session_id}", err=True)
        click.echo("Use 'datasight export --list-sessions' to see available sessions.", err=True)
        sys.exit(1)

    data = json_mod.loads(session_path.read_text(encoding="utf-8"))
    events = data.get("events", [])
    title = data.get("title", "datasight session")

    if not events:
        click.echo("Error: Session has no events.", err=True)
        sys.exit(1)

    exclude_indices: set[int] | None = None
    if exclude:
        try:
            exclude_indices = {int(x.strip()) for x in exclude.split(",")}
        except ValueError:
            click.echo("Error: --exclude must be comma-separated integers.", err=True)
            sys.exit(1)

    fmt = output_format.lower()
    if fmt == "py":
        from datasight.export import export_session_python

        settings, _ = _resolve_settings(project_dir)
        db_path = _resolve_db_path(settings, project_dir)
        script = export_session_python(
            events,
            title=title,
            db_path=db_path,
            db_mode=settings.database.mode or "duckdb",
            exclude_indices=exclude_indices,
        )
        if not output_path:
            output_path = f"{session_id[:20]}.py"
        Path(output_path).write_text(script, encoding="utf-8")
        click.echo(f"Session exported to {output_path}")
        return

    from datasight.export import export_session_html

    html = export_session_html(events, title=title, exclude_indices=exclude_indices)

    if not output_path:
        output_path = f"{session_id[:20]}.html"

    Path(output_path).write_text(html, encoding="utf-8")
    click.echo(f"Session exported to {output_path}")
