"""Versioned session archive helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
import json
from pathlib import Path
import re
from typing import Any
import zipfile

from datasight import __version__

SESSION_ARCHIVE_FORMAT = "datasight-session-archive"
SESSION_ARCHIVE_VERSION = 1
_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def validate_session_archive_id(session_id: str) -> str:
    """Validate a session ID used in archive export/import."""
    if not _SESSION_ID_RE.match(session_id) or len(session_id) > 128:
        raise ValueError(f"Invalid session ID: {session_id!r}")
    return session_id


def _relative_or_absolute_path(path: str, project_dir: str) -> dict[str, Any] | None:
    raw_path = str(path or "").strip()
    if not raw_path:
        return None

    target = Path(raw_path)
    if not target.is_absolute():
        target = (Path(project_dir) / target).resolve()
    else:
        target = target.resolve()

    project_root = Path(project_dir).resolve()
    try:
        rel_path = target.relative_to(project_root)
    except ValueError:
        return {"path": str(target), "path_kind": "absolute"}
    return {"path": str(rel_path), "path_kind": "relative"}


def _resolve_db_file_path(path: str, project_dir: str) -> Path:
    raw_path = str(path or "").strip()
    if not raw_path:
        return Path(project_dir).resolve()
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = Path(project_dir) / candidate
    return candidate.resolve()


def _safe_project_relative_path(path: str) -> Path:
    candidate = Path(str(path))
    if candidate.is_absolute():
        raise ValueError(f"Archive restore path must be relative: {path!r}")

    clean_parts: list[str] = []
    for part in candidate.parts:
        if part in ("", "."):
            continue
        if part == "..":
            raise ValueError(f"Archive restore path cannot traverse upward: {path!r}")
        clean_parts.append(part)

    if not clean_parts:
        raise ValueError(f"Archive restore path is empty: {path!r}")
    return Path(*clean_parts)


def build_session_archive(
    *,
    session_id: str,
    conversation: dict[str, Any],
    dashboard: dict[str, Any] | None,
    project_dir: str,
    db_mode: str = "",
    db_path: str = "",
    include_data: bool = False,
) -> bytes:
    """Build a portable zip archive for a saved datasight session."""
    session_id = validate_session_archive_id(session_id)

    conversation_payload = dict(conversation)
    dashboard_payload = dashboard if isinstance(dashboard, dict) else {}
    conversation_payload["dashboard"] = dashboard_payload

    source_data: dict[str, Any] = {}
    normalized_db_mode = str(db_mode or "").strip()
    embedded_files: list[tuple[Path, str]] = []
    if normalized_db_mode:
        db_ref = {"mode": normalized_db_mode, "embedded": False}
        path_ref = _relative_or_absolute_path(db_path, project_dir)
        if path_ref is not None:
            db_ref.update(path_ref)
        if include_data:
            if normalized_db_mode not in {"duckdb", "sqlite"}:
                raise ValueError(
                    "--include-data is only supported for DuckDB and SQLite projects."
                )
            resolved_db_path = _resolve_db_file_path(db_path, project_dir)
            if not resolved_db_path.exists() or not resolved_db_path.is_file():
                raise ValueError(f"Database file not found for --include-data: {resolved_db_path}")
            archive_name = (
                path_ref["path"]
                if path_ref and path_ref.get("path_kind") == "relative"
                else resolved_db_path.name
            )
            archive_member = f"data/{archive_name}"
            db_ref["embedded"] = True
            db_ref["archive_path"] = archive_member
            embedded_files.append((resolved_db_path, archive_member))
        source_data["database"] = db_ref

    manifest = {
        "format": SESSION_ARCHIVE_FORMAT,
        "archive_version": SESSION_ARCHIVE_VERSION,
        "datasight_version": __version__,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "session_id": session_id,
        "title": conversation_payload.get("title", "Untitled"),
        "artifacts": [
            "manifest.json",
            "session/conversation.json",
            "session/dashboard.json",
            "metadata/source_data.json",
        ],
    }
    manifest["includes_data"] = bool(embedded_files)
    manifest["artifacts"].extend(archive_path for _, archive_path in embedded_files)

    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest, indent=2, sort_keys=True))
        zf.writestr(
            "session/conversation.json",
            json.dumps(conversation_payload, indent=2, sort_keys=True),
        )
        zf.writestr(
            "session/dashboard.json",
            json.dumps(dashboard_payload, indent=2, sort_keys=True),
        )
        zf.writestr(
            "metadata/source_data.json",
            json.dumps(source_data, indent=2, sort_keys=True),
        )
        for file_path, archive_path in embedded_files:
            zf.write(file_path, archive_path)
    return buf.getvalue()


def read_session_archive(archive: bytes) -> dict[str, Any]:
    """Read and validate a datasight session archive."""
    with zipfile.ZipFile(BytesIO(archive)) as zf:
        try:
            manifest = json.loads(zf.read("manifest.json"))
            conversation = json.loads(zf.read("session/conversation.json"))
            dashboard = json.loads(zf.read("session/dashboard.json"))
            source_data = json.loads(zf.read("metadata/source_data.json"))
        except KeyError as err:
            raise ValueError(f"Archive is missing required entry: {err.args[0]}") from err
        except json.JSONDecodeError as err:
            raise ValueError(f"Archive JSON is invalid: {err}") from err

        if manifest.get("format") != SESSION_ARCHIVE_FORMAT:
            raise ValueError("Not a datasight session archive.")
        version = manifest.get("archive_version")
        if version != SESSION_ARCHIVE_VERSION:
            raise ValueError(
                f"Unsupported session archive version {version!r}. "
                f"Expected {SESSION_ARCHIVE_VERSION}."
            )

        if not isinstance(conversation, dict):
            raise ValueError("Archive conversation payload must be an object.")
        if not isinstance(dashboard, dict):
            raise ValueError("Archive dashboard payload must be an object.")
        if not isinstance(source_data, dict):
            raise ValueError("Archive source_data payload must be an object.")

        session_id = validate_session_archive_id(str(manifest.get("session_id") or ""))
        conversation["dashboard"] = dashboard
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
    """Import a session archive into a datasight project directory."""
    payload = read_session_archive(archive)
    imported_session_id = validate_session_archive_id(
        session_id if session_id is not None else payload["session_id"]
    )

    project_root = Path(project_dir).resolve()
    datasight_dir = project_root / ".datasight"
    conversations_dir = datasight_dir / "conversations"
    conversations_dir.mkdir(parents=True, exist_ok=True)

    conversation_path = conversations_dir / f"{imported_session_id}.json"
    if conversation_path.exists() and not overwrite:
        raise ValueError(
            f"Session {imported_session_id!r} already exists in {project_root}. "
            "Pass --overwrite to replace it."
        )

    conversation = dict(payload["conversation"])
    dashboard = payload["dashboard"]
    conversation["dashboard"] = dashboard

    conversation_path.write_text(
        json.dumps(conversation, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (datasight_dir / "dashboard.json").write_text(
        json.dumps(dashboard, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    restored_paths: list[Path] = []
    source_data = payload["source_data"]
    database = source_data.get("database") if isinstance(source_data, dict) else None
    if isinstance(database, dict) and database.get("embedded"):
        archive_path = database.get("archive_path")
        if not isinstance(archive_path, str) or not archive_path:
            raise ValueError("Archive metadata for embedded database is incomplete.")
        restore_rel_path_raw = (
            database.get("path")
            if database.get("path_kind") == "relative" and isinstance(database.get("path"), str)
            else Path(archive_path).name
        )
        restore_rel_path = _safe_project_relative_path(str(restore_rel_path_raw))
        restore_path = project_root / restore_rel_path
        restore_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(BytesIO(archive)) as zf:
            try:
                restore_path.write_bytes(zf.read(archive_path))
            except KeyError as err:
                raise ValueError(f"Archive is missing embedded data file: {archive_path}") from err
        restored_paths.append(restore_path)

        db_mode = str(database.get("mode") or "").strip()
        if db_mode in {"duckdb", "sqlite"}:
            env_path = project_root / ".env"
            env_path.write_text(
                f"DB_MODE={db_mode}\nDB_PATH={restore_rel_path.as_posix()}\n",
                encoding="utf-8",
            )

    return {
        "session_id": imported_session_id,
        "conversation_path": conversation_path,
        "dashboard_path": datasight_dir / "dashboard.json",
        "manifest": payload["manifest"],
        "restored_paths": restored_paths,
    }
