"""Session archive CLI commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click

from datasight.cli import _epilog, _resolve_db_path, _resolve_settings
from datasight.session_archive import (
    build_session_archive,
    import_session_archive,
    validate_session_archive_id,
)

_PROJECT_DIR_OPT = click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(exists=True, file_okay=False),
    default=".",
    help="Project directory containing .datasight/ state (default: cwd).",
)


def _conversation_dir(project_dir: str) -> Path:
    return Path(project_dir).resolve() / ".datasight" / "conversations"


def _load_session(project_dir: str, session_id: str) -> dict[str, Any]:
    validate_session_archive_id(session_id)
    path = _conversation_dir(project_dir) / f"{session_id}.json"
    if not path.exists():
        raise click.ClickException(
            f"Session not found: {session_id}. Use 'datasight session list' to see available sessions."
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as err:
        raise click.ClickException(f"Session JSON is invalid: {err}") from err
    if not isinstance(data, dict):
        raise click.ClickException("Session JSON must be an object.")
    return data


@click.group(
    epilog=_epilog(
        """
        Examples:

            datasight session list
            datasight session export abc123 --output-path analysis.zip
            datasight session export abc123 --include-data
            datasight session import analysis.zip
            datasight session import analysis.zip --session-id copied-session --overwrite
        """
    )
)
def session() -> None:
    """Export and import shareable datasight session archives."""


@click.command(name="list")
@_PROJECT_DIR_OPT
def session_list(project_dir: str) -> None:
    """List saved sessions available for export."""
    conv_dir = _conversation_dir(project_dir)
    if not conv_dir.exists():
        click.echo("No conversations found.")
        return

    sessions: list[dict[str, Any]] = []
    for path in sorted(conv_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        events = data.get("events", [])
        if not events and not data.get("dashboard"):
            continue
        sessions.append(
            {
                "id": path.stem,
                "title": data.get("title", "Untitled"),
                "messages": sum(1 for event in events if event.get("event") == "user_message"),
            }
        )

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
    for item in sessions:
        table.add_row(item["id"], item["title"], str(item["messages"]))
    console.print(table)


@click.command(name="export")
@click.argument("session_id")
@_PROJECT_DIR_OPT
@click.option(
    "--output-path",
    "output_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Output archive path. Defaults to <session_id>.zip in the current directory.",
)
@click.option(
    "--include-data",
    is_flag=True,
    help="Embed the DuckDB or SQLite database file into the archive for a runnable import.",
)
def session_export(
    session_id: str,
    project_dir: str,
    output_path: Path | None,
    include_data: bool,
) -> None:
    """Export SESSION_ID as a versioned datasight session archive."""
    project_root = str(Path(project_dir).resolve())
    session_data = _load_session(project_root, session_id)
    resolved_output_path = output_path or Path(f"{session_id[:20]}.zip")

    settings, _ = _resolve_settings(project_root)
    archive = build_session_archive(
        session_id=session_id,
        conversation=session_data,
        dashboard=session_data.get("dashboard") or {},
        project_dir=project_root,
        db_mode=settings.database.mode or "duckdb",
        db_path=_resolve_db_path(settings, project_root),
        include_data=include_data,
    )
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_output_path.write_bytes(archive)
    click.echo(f"Session archive exported to {resolved_output_path}")


@click.command(name="import")
@click.argument("archive_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@_PROJECT_DIR_OPT
@click.option(
    "--session-id",
    default=None,
    help="Import under this session ID instead of the archived ID.",
)
@click.option("--overwrite", is_flag=True, help="Replace an existing session with the same ID.")
def session_import(
    archive_path: Path,
    project_dir: str,
    session_id: str | None,
    overwrite: bool,
) -> None:
    """Import a datasight session archive into PROJECT_DIR."""
    try:
        result = import_session_archive(
            archive=archive_path.read_bytes(),
            project_dir=str(Path(project_dir).resolve()),
            session_id=session_id,
            overwrite=overwrite,
        )
    except ValueError as err:
        raise click.ClickException(str(err)) from err

    click.echo(
        f"Imported session {result['session_id']} into {result['conversation_path'].parent.parent}"
    )


session.add_command(session_list)
session.add_command(session_export)
session.add_command(session_import)
