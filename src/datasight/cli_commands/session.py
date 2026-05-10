"""Session archive CLI commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click
from rich.console import Console
from rich.table import Table

from datasight.cli_helpers import format_epilog
from datasight.session_archive import (
    import_session_archive,
    validate_session_archive_id,
    write_session_archive,
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
        msg = (
            f"Session not found: {session_id}. "
            "Use 'datasight session list' to see available sessions."
        )
        raise click.ClickException(msg)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as err:
        msg = f"Session JSON is invalid: {err}"
        raise click.ClickException(msg) from err
    if not isinstance(data, dict):
        msg = "Session JSON must be an object."
        raise click.ClickException(msg)
    return data


@click.group(
    epilog=format_epilog(
        """
        Examples:

            datasight session list
            datasight session export abc123 --output-path analysis.zip
            datasight session import analysis.zip
            datasight session import analysis.zip --session-id copied-session --overwrite
        """
    )
)
def session() -> None:
    """Export and import shareable datasight session archives.

    Archives carry the conversation transcript and per-session dashboard
    only — never .env or LLM credentials, and never the underlying
    database. Recipients need to bring their own data.
    """


@click.command(name="list")
@_PROJECT_DIR_OPT
def session_list(project_dir: str) -> None:
    """List saved sessions available for export."""
    conv_dir = _conversation_dir(project_dir)
    if not conv_dir.exists():
        click.echo("No conversations found.")
        return

    sessions: list[dict[str, Any]] = []
    invalid_paths: list[Path] = []
    for path in sorted(conv_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            # Bad JSON is the deliberate skip case; OS-level errors
            # (missing file, permission denied) are real problems and are
            # allowed to propagate.
            invalid_paths.append(path)
            continue
        if not isinstance(data, dict):
            invalid_paths.append(path)
            continue
        events = data.get("events") or []
        if not events and not data.get("dashboard"):
            continue
        sessions.append(
            {
                "id": path.stem,
                "title": data.get("title", "Untitled"),
                "messages": sum(1 for e in events if e.get("event") == "user_message"),
            }
        )

    def _warn() -> None:
        for p in invalid_paths:
            click.echo(f"Warning: skipped unreadable session file {p.name}", err=True)

    if not sessions:
        _warn()
        click.echo("No conversations found.")
        return

    console = Console()
    table = Table(title="Available Sessions")
    table.add_column("Session ID", style="cyan", no_wrap=True)
    table.add_column("Title", overflow="fold")
    table.add_column("Messages", justify="right")
    for item in sessions:
        table.add_row(item["id"], item["title"], str(item["messages"]))
    console.print(table)
    _warn()


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
def session_export(
    session_id: str,
    project_dir: str,
    output_path: Path | None,
) -> None:
    """Export SESSION_ID as a versioned datasight session archive."""
    project_root = str(Path(project_dir).resolve())
    session_data = _load_session(project_root, session_id)
    resolved_output_path = output_path or Path(f"{session_id}.zip")

    try:
        write_session_archive(
            output=resolved_output_path,
            session_id=session_id,
            conversation=session_data,
        )
    except ValueError as err:
        raise click.ClickException(str(err)) from err

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
    target_root = Path(project_dir).resolve()
    try:
        # Pass the path through so import_session_archive streams from
        # disk instead of materializing the whole archive in memory.
        result = import_session_archive(
            archive=archive_path,
            project_dir=str(target_root),
            session_id=session_id,
            overwrite=overwrite,
        )
    except ValueError as err:
        raise click.ClickException(str(err)) from err

    click.echo(f"Imported session {result['session_id']} into {target_root}")


session.add_command(session_list)
session.add_command(session_export)
session.add_command(session_import)
