"""Versioned session archive helpers.

A session archive is a zip file with this layout::

    manifest.json              # format/version/exported_at/session_id/title
    session/conversation.json  # the .datasight/conversations/<id>.json file,
                               # which already embeds the per-session dashboard
                               # under conversation["dashboard"]
    metadata/source_data.json  # database mode + path the session was authored
                               # against, plus an "archive_path" pointer if a
                               # data file was embedded
    data/<basename>            # only present when --include-data was passed

The on-disk shape of conversations and per-session dashboards is the
authoritative source of truth (see ``ConversationStore`` and
``DashboardStore`` in ``datasight.web.app``). This module just hands those
files in and out of a zip with a manifest.

Privacy
-------
Archives are intended to be shared with collaborators. They deliberately
do **not** include ``.env`` and never read LLM credentials. Database paths
that fall outside the project directory are recorded as basenames only so
the exporter's filesystem layout is not leaked.
"""

from __future__ import annotations

import json
import re
import shutil
import zipfile
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

from datasight import __version__

ARCHIVE_FORMAT = "datasight-session-archive"
ARCHIVE_VERSION = 1
_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_FILE_DB_MODES = frozenset({"duckdb", "sqlite"})

# Backwards-compatible aliases for callers/tests that import the older names.
SESSION_ARCHIVE_FORMAT = ARCHIVE_FORMAT
SESSION_ARCHIVE_VERSION = ARCHIVE_VERSION


def validate_session_archive_id(session_id: str) -> str:
    """Validate a session ID for archive export/import.

    Mirrors the regex used by ``datasight.web.app.validate_session_id`` so
    archives can round-trip in and out of the web app's conversation store.
    """
    if not _SESSION_ID_RE.match(session_id) or len(session_id) > 128:
        raise ValueError(f"Invalid session ID: {session_id!r}")
    return session_id


def _safe_relative_member(name: str) -> Path:
    """Reject absolute paths or anything that escapes via ``..``."""
    candidate = Path(name)
    if candidate.is_absolute():
        raise ValueError(f"Archive restore path must be relative: {name!r}")
    parts: list[str] = []
    for part in candidate.parts:
        if part in ("", "."):
            continue
        if part == "..":
            raise ValueError(f"Archive restore path cannot traverse upward: {name!r}")
        parts.append(part)
    if not parts:
        raise ValueError(f"Archive restore path is empty: {name!r}")
    return Path(*parts)


def _resolve_db_source(db_path: str, project_root: Path) -> Path:
    candidate = Path(db_path)
    if not candidate.is_absolute():
        candidate = project_root / candidate
    return candidate.resolve()


def _archive_db_reference(
    db_mode: str,
    db_path: str,
    project_root: Path,
) -> dict[str, Any] | None:
    mode = (db_mode or "").strip()
    if not mode:
        return None
    ref: dict[str, Any] = {"mode": mode, "embedded": False}
    raw = (db_path or "").strip()
    if not raw:
        return ref
    target = _resolve_db_source(raw, project_root)
    try:
        rel = target.relative_to(project_root)
        ref["path"] = rel.as_posix()
        ref["path_kind"] = "relative"
    except ValueError:
        # Don't leak the exporter's absolute filesystem layout. Record
        # only the basename so the importer can still pick a sensible
        # destination without learning where the source lived.
        ref["path"] = target.name
        ref["path_kind"] = "external"
    return ref


def build_session_archive(
    *,
    session_id: str,
    conversation: dict[str, Any],
    dashboard: dict[str, Any] | None = None,
    project_dir: str,
    db_mode: str = "",
    db_path: str = "",
    include_data: bool = False,
) -> bytes:
    """Build a portable zip archive for a saved datasight session.

    ``conversation`` is the on-disk conversation JSON, which already
    embeds the per-session dashboard under ``conversation["dashboard"]``.
    ``dashboard`` is an optional override for callers that want to attach
    a dashboard explicitly.
    """
    validate_session_archive_id(session_id)
    project_root = Path(project_dir).resolve()

    payload = dict(conversation)
    if dashboard is not None:
        payload["dashboard"] = dashboard

    db_ref = _archive_db_reference(db_mode, db_path, project_root)
    embedded_db: tuple[Path, str] | None = None
    if include_data:
        if db_ref is None or db_ref.get("mode") not in _FILE_DB_MODES:
            raise ValueError("--include-data is only supported for DuckDB and SQLite projects.")
        if not (db_path or "").strip():
            raise ValueError("--include-data requires a configured database path.")
        source = _resolve_db_source(db_path, project_root)
        if not source.is_file():
            raise ValueError(f"Database file not found for --include-data: {source}")
        member = f"data/{source.name}"
        db_ref["embedded"] = True
        db_ref["archive_path"] = member
        embedded_db = (source, member)

    source_data: dict[str, Any] = {}
    if db_ref is not None:
        source_data["database"] = db_ref

    manifest = {
        "format": ARCHIVE_FORMAT,
        "archive_version": ARCHIVE_VERSION,
        "datasight_version": __version__,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "session_id": session_id,
        "title": payload.get("title", "Untitled"),
        "includes_data": embedded_db is not None,
    }

    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest, indent=2, sort_keys=True))
        zf.writestr(
            "session/conversation.json",
            json.dumps(payload, indent=2, sort_keys=True),
        )
        zf.writestr(
            "metadata/source_data.json",
            json.dumps(source_data, indent=2, sort_keys=True),
        )
        if embedded_db is not None:
            source, member = embedded_db
            zf.write(source, member)
    return buf.getvalue()


def read_session_archive(archive: bytes) -> dict[str, Any]:
    """Read and validate a datasight session archive without extracting it."""
    with zipfile.ZipFile(BytesIO(archive)) as zf:
        try:
            manifest = json.loads(zf.read("manifest.json"))
        except KeyError:
            raise ValueError("Archive is missing manifest.json.") from None
        except json.JSONDecodeError as err:
            raise ValueError(f"Archive manifest.json is invalid: {err}") from err

        if manifest.get("format") != ARCHIVE_FORMAT:
            raise ValueError("Not a datasight session archive.")
        version = manifest.get("archive_version")
        # Archive versions are integers; equal majors are read, greater
        # majors are rejected with a clear message so future writers can
        # extend the schema additively without retroactively breaking
        # older readers.
        if not isinstance(version, int) or version > ARCHIVE_VERSION:
            raise ValueError(
                f"Unsupported session archive version {version!r} "
                f"(this build reads up to version {ARCHIVE_VERSION})."
            )

        try:
            conversation = json.loads(zf.read("session/conversation.json"))
            source_data = json.loads(zf.read("metadata/source_data.json"))
        except KeyError as err:
            raise ValueError(f"Archive is missing required entry: {err.args[0]}") from err
        except json.JSONDecodeError as err:
            raise ValueError(f"Archive JSON is invalid: {err}") from err

    if not isinstance(conversation, dict):
        raise ValueError("Archive conversation payload must be an object.")
    if not isinstance(source_data, dict):
        raise ValueError("Archive source_data payload must be an object.")

    session_id = validate_session_archive_id(str(manifest.get("session_id") or ""))
    dashboard = conversation.get("dashboard")
    if not isinstance(dashboard, dict):
        dashboard = {}
    return {
        "manifest": manifest,
        "session_id": session_id,
        "conversation": conversation,
        "dashboard": dashboard,
        "source_data": source_data,
    }


def import_session_archive(
    *,
    archive: bytes,
    project_dir: str,
    session_id: str | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Import a session archive into a datasight project directory.

    Writes the conversation file under ``.datasight/conversations/`` and,
    if the archive embeds a database file, restores it alongside the
    project. The active project-wide ``.datasight/dashboard.json`` and
    existing ``.env`` are deliberately left untouched — the per-session
    dashboard travels inside the conversation JSON, and the web app reads
    it from there via ``?session_id=...``.

    A minimal ``.env`` is only written when the archive includes data and
    the project has no ``.env`` yet, so a fresh-project import is
    immediately runnable.
    """
    project_root = Path(project_dir).resolve()
    payload = read_session_archive(archive)
    target_session_id = validate_session_archive_id(
        session_id if session_id is not None else payload["session_id"]
    )

    conversations_dir = project_root / ".datasight" / "conversations"
    conversation_path = conversations_dir / f"{target_session_id}.json"
    if conversation_path.exists() and not overwrite:
        raise ValueError(
            f"Session {target_session_id!r} already exists in {project_root}. "
            "Pass --overwrite to replace it."
        )

    database = payload["source_data"].get("database")
    restore_plan: tuple[Path, str, str] | None = None
    if isinstance(database, dict) and database.get("embedded"):
        member = database.get("archive_path")
        if not isinstance(member, str) or not member:
            raise ValueError("Archive metadata for embedded database is incomplete.")
        mode = str(database.get("mode") or "").strip()
        if mode not in _FILE_DB_MODES:
            raise ValueError(f"Cannot restore embedded data for db mode {mode!r}.")

        # Pick the restore location. Honor the recorded relative path so a
        # within-project DB lands at the same place; for absolute or
        # missing paths, drop it next to the project under the archive
        # member's basename so the exporter can never dictate an absolute
        # filesystem location on the importer.
        recorded = database.get("path") if isinstance(database.get("path"), str) else None
        if database.get("path_kind") == "relative" and recorded:
            rel = _safe_relative_member(recorded)
        else:
            rel = Path(Path(member).name)
        target = project_root / rel
        if target.exists() and not overwrite:
            raise ValueError(
                f"Database file {target} already exists. Pass --overwrite to replace it."
            )
        restore_plan = (target, member, mode)

    # Only mutate the project once every safety check has passed.
    conversations_dir.mkdir(parents=True, exist_ok=True)
    conversation_path.write_text(
        json.dumps(payload["conversation"], indent=2, sort_keys=True),
        encoding="utf-8",
    )

    restored_db_path: Path | None = None
    env_written = False
    if restore_plan is not None:
        target, member, mode = restore_plan
        target.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(BytesIO(archive)) as zf:
            with zf.open(member) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst)
        restored_db_path = target

        env_path = project_root / ".env"
        if not env_path.exists():
            rel_db = target.relative_to(project_root).as_posix()
            env_path.write_text(
                f"DB_MODE={mode}\nDB_PATH={rel_db}\n",
                encoding="utf-8",
            )
            env_written = True

    return {
        "session_id": target_session_id,
        "conversation_path": conversation_path,
        "manifest": payload["manifest"],
        "restored_db_path": restored_db_path,
        "env_written": env_written,
    }
