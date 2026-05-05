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

import contextlib
import json
import os
import re
import shutil
import uuid
import zipfile
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import IO, Any, ContextManager

from datasight import __version__

ARCHIVE_FORMAT = "datasight-session-archive"
ARCHIVE_VERSION = 1
_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_FILE_DB_MODES = frozenset({"duckdb", "sqlite"})

# Backwards-compatible aliases for callers/tests that import the older names.
SESSION_ARCHIVE_FORMAT = ARCHIVE_FORMAT
SESSION_ARCHIVE_VERSION = ARCHIVE_VERSION

ArchiveSource = bytes | bytearray | str | os.PathLike[str]


def validate_session_archive_id(session_id: str) -> str:
    """Validate a session ID for archive export/import.

    Mirrors the regex used by ``datasight.web.app.validate_session_id`` so
    archives can round-trip in and out of the web app's conversation store.
    """
    if not _SESSION_ID_RE.match(session_id) or len(session_id) > 128:
        msg = f"Invalid session ID: {session_id!r}"
        raise ValueError(msg)
    return session_id


def _safe_relative_member(name: str) -> Path:
    """Reject absolute paths or anything that escapes via ``..``."""
    candidate = Path(name)
    if candidate.is_absolute():
        msg = f"Archive restore path must be relative: {name!r}"
        raise ValueError(msg)
    parts: list[str] = []
    for part in candidate.parts:
        if part in ("", "."):
            continue
        if part == "..":
            msg = f"Archive restore path cannot traverse upward: {name!r}"
            raise ValueError(msg)
        parts.append(part)
    if not parts:
        msg = f"Archive restore path is empty: {name!r}"
        raise ValueError(msg)
    return Path(*parts)


def _ensure_within_project(target: Path, project_root: Path) -> None:
    """Refuse to write to a path that resolves outside ``project_root``.

    ``target`` may not exist yet; resolve it relative to a real ancestor so
    a parent symlink that escapes the project tree still gets caught.
    """
    probe = target
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    resolved_anchor = probe.resolve()
    resolved_target = resolved_anchor / target.relative_to(probe)
    if not resolved_target.is_relative_to(project_root.resolve()):
        msg = f"Archive restore path escapes the project directory: {target}"
        raise ValueError(msg)


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


def _build_manifest(
    *,
    session_id: str,
    title: str,
    includes_data: bool,
) -> dict[str, Any]:
    return {
        "format": ARCHIVE_FORMAT,
        "archive_version": ARCHIVE_VERSION,
        "datasight_version": __version__,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "session_id": session_id,
        "title": title,
        "includes_data": includes_data,
    }


def _write_archive_to(
    sink: IO[bytes],
    *,
    session_id: str,
    conversation: dict[str, Any],
    dashboard: dict[str, Any] | None,
    project_dir: str,
    db_mode: str,
    db_path: str,
    include_data: bool,
) -> None:
    validate_session_archive_id(session_id)
    project_root = Path(project_dir).resolve()

    payload = dict(conversation)
    if dashboard is not None:
        payload["dashboard"] = dashboard

    db_ref = _archive_db_reference(db_mode, db_path, project_root)
    embedded_db: tuple[Path, str] | None = None
    if include_data:
        if db_ref is None or db_ref.get("mode") not in _FILE_DB_MODES:
            msg = "--include-data is only supported for DuckDB and SQLite projects."
            raise ValueError(msg)
        if not (db_path or "").strip():
            msg = "--include-data requires a configured database path."
            raise ValueError(msg)
        source = _resolve_db_source(db_path, project_root)
        if not source.is_file():
            msg = f"Database file not found for --include-data: {source}"
            raise ValueError(msg)
        member = f"data/{source.name}"
        db_ref["embedded"] = True
        db_ref["archive_path"] = member
        embedded_db = (source, member)

    source_data: dict[str, Any] = {}
    if db_ref is not None:
        source_data["database"] = db_ref

    manifest = _build_manifest(
        session_id=session_id,
        title=str(payload.get("title", "Untitled")),
        includes_data=embedded_db is not None,
    )

    with zipfile.ZipFile(sink, "w", compression=zipfile.ZIP_DEFLATED) as zf:
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
            # zf.write streams the file from disk; no full buffer in memory.
            zf.write(source, member)


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

    Buffers the archive in memory and returns its bytes — convenient for
    callers that don't have an output path. For ``--include-data`` exports
    of large databases prefer :func:`write_session_archive`, which streams
    directly to disk and avoids materializing the whole archive in RAM.
    """
    buf = BytesIO()
    _write_archive_to(
        buf,
        session_id=session_id,
        conversation=conversation,
        dashboard=dashboard,
        project_dir=project_dir,
        db_mode=db_mode,
        db_path=db_path,
        include_data=include_data,
    )
    return buf.getvalue()


def write_session_archive(
    *,
    output: str | os.PathLike[str],
    session_id: str,
    conversation: dict[str, Any],
    dashboard: dict[str, Any] | None = None,
    project_dir: str,
    db_mode: str = "",
    db_path: str = "",
    include_data: bool = False,
) -> Path:
    """Stream a session archive to ``output``.

    Writes to a sibling ``.part-<rand>`` file and renames into place on
    success so a partial archive doesn't leave a misleading file behind
    on failure. Returns the final path.
    """
    final_path = Path(output)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = final_path.with_name(f"{final_path.name}.part-{uuid.uuid4().hex[:8]}")
    try:
        with tmp_path.open("wb") as fh:
            _write_archive_to(
                fh,
                session_id=session_id,
                conversation=conversation,
                dashboard=dashboard,
                project_dir=project_dir,
                db_mode=db_mode,
                db_path=db_path,
                include_data=include_data,
            )
        os.replace(tmp_path, final_path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            tmp_path.unlink()
        raise
    return final_path


def _open_archive(archive: ArchiveSource) -> ContextManager[zipfile.ZipFile]:
    """Return a ZipFile context manager for ``archive`` (bytes or path).

    Path-based archives stream from disk; bytes-based archives wrap
    in BytesIO. ``BadZipFile`` is normalized to ``ValueError`` so the
    CLI surfaces a clean error.
    """

    @contextlib.contextmanager
    def _opener():  # type: ignore[no-untyped-def]
        try:
            if isinstance(archive, (bytes, bytearray)):
                with zipfile.ZipFile(BytesIO(archive), "r") as zf:
                    yield zf
            else:
                with zipfile.ZipFile(os.fspath(archive), "r") as zf:
                    yield zf
        except zipfile.BadZipFile as err:
            msg = f"Not a valid zip archive: {err}"
            raise ValueError(msg) from err

    return _opener()


def _read_manifest_payload(
    zf: zipfile.ZipFile,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    try:
        manifest = json.loads(zf.read("manifest.json"))
    except KeyError:
        msg = "Archive is missing manifest.json."
        raise ValueError(msg) from None
    except json.JSONDecodeError as err:
        msg = f"Archive manifest.json is invalid: {err}"
        raise ValueError(msg) from err

    if not isinstance(manifest, dict):
        msg = "Archive manifest.json must be a JSON object."
        raise ValueError(msg)
    if manifest.get("format") != ARCHIVE_FORMAT:
        msg = "Not a datasight session archive."
        raise ValueError(msg)
    version = manifest.get("archive_version")
    # Same-major archives are read; greater majors are rejected so future
    # writers can extend the schema additively without breaking older
    # readers retroactively.
    if not isinstance(version, int) or version > ARCHIVE_VERSION:
        msg = (
            f"Unsupported session archive version {version!r} "
            f"(this build reads up to version {ARCHIVE_VERSION})."
        )
        raise ValueError(msg)

    try:
        conversation = json.loads(zf.read("session/conversation.json"))
        source_data = json.loads(zf.read("metadata/source_data.json"))
    except KeyError as err:
        missing = err.args[0] if err.args else "archive entry"
        msg = f"Archive is missing required entry: {missing}"
        raise ValueError(msg) from err
    except json.JSONDecodeError as err:
        msg = f"Archive JSON is invalid: {err}"
        raise ValueError(msg) from err

    if not isinstance(conversation, dict):
        msg = "Archive conversation payload must be an object."
        raise ValueError(msg)
    if not isinstance(source_data, dict):
        msg = "Archive source_data payload must be an object."
        raise ValueError(msg)

    return manifest, conversation, source_data


def read_session_archive(archive: ArchiveSource) -> dict[str, Any]:
    """Read and validate a datasight session archive without extracting it.

    ``archive`` may be raw bytes or a path; path inputs stream from disk.
    """
    with _open_archive(archive) as zf:
        manifest, conversation, source_data = _read_manifest_payload(zf)

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


def _plan_db_restore(
    database: dict[str, Any],
    project_root: Path,
    *,
    overwrite: bool,
) -> tuple[Path, str, str]:
    member = database.get("archive_path")
    if not isinstance(member, str) or not member:
        msg = "Archive metadata for embedded database is incomplete."
        raise ValueError(msg)
    mode = str(database.get("mode") or "").strip()
    if mode not in _FILE_DB_MODES:
        msg = f"Cannot restore embedded data for db mode {mode!r}."
        raise ValueError(msg)

    # Honor the recorded relative path so a within-project DB lands at
    # the same place; for "external" or missing paths, drop the file
    # next to the project under the archive member's basename so the
    # exporter can never dictate an absolute layout on the importer.
    recorded = database.get("path") if isinstance(database.get("path"), str) else None
    if database.get("path_kind") == "relative" and recorded:
        rel = _safe_relative_member(recorded)
    else:
        rel = Path(Path(member).name)
    target = project_root / rel
    _ensure_within_project(target, project_root)
    if target.exists() and not overwrite:
        msg = f"Database file {target} already exists. Pass --overwrite to replace it."
        raise ValueError(msg)
    return target, member, mode


def import_session_archive(
    *,
    archive: ArchiveSource,
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

    The import is staged via ``.part-<rand>`` files and renamed into
    place on success, so a failure mid-extraction does not leave a
    half-imported session on disk.
    """
    project_root = Path(project_dir).resolve()

    with _open_archive(archive) as zf:
        manifest, conversation, source_data = _read_manifest_payload(zf)

        target_session_id = validate_session_archive_id(
            session_id if session_id is not None else str(manifest.get("session_id") or "")
        )

        conversations_dir = project_root / ".datasight" / "conversations"
        conversation_path = conversations_dir / f"{target_session_id}.json"
        if conversation_path.exists() and not overwrite:
            msg = (
                f"Session {target_session_id!r} already exists in {project_root}. "
                "Pass --overwrite to replace it."
            )
            raise ValueError(msg)

        database = source_data.get("database")
        restore_plan: tuple[Path, str, str] | None = None
        if isinstance(database, dict) and database.get("embedded"):
            restore_plan = _plan_db_restore(database, project_root, overwrite=overwrite)
            # Fail fast if the metadata points at a member that's not
            # actually in the zip — better than crashing mid-extract.
            try:
                zf.getinfo(restore_plan[1])
            except KeyError as err:
                msg = f"Archive is missing embedded data file: {restore_plan[1]}"
                raise ValueError(msg) from err

        conversations_dir.mkdir(parents=True, exist_ok=True)
        suffix = f".part-{uuid.uuid4().hex[:8]}"
        conv_tmp = conversation_path.with_name(conversation_path.name + suffix)

        db_tmp: Path | None = None
        if restore_plan is not None:
            target, _, _ = restore_plan
            target.parent.mkdir(parents=True, exist_ok=True)
            db_tmp = target.with_name(target.name + suffix)

        try:
            # Stage: write both files to .part siblings.
            conv_tmp.write_text(
                json.dumps(conversation, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            if restore_plan is not None and db_tmp is not None:
                _, member, _ = restore_plan
                with zf.open(member) as src, db_tmp.open("wb") as dst:
                    shutil.copyfileobj(src, dst)

            # Commit: atomic renames. DB first so the conversation only
            # appears once its referenced data is in place.
            if restore_plan is not None and db_tmp is not None:
                target, _, _ = restore_plan
                os.replace(db_tmp, target)
                db_tmp = None
            os.replace(conv_tmp, conversation_path)
        except BaseException:
            for tmp in (conv_tmp, db_tmp):
                if tmp is None:
                    continue
                with contextlib.suppress(FileNotFoundError):
                    tmp.unlink()
            raise

    restored_db_path: Path | None = None
    env_written = False
    if restore_plan is not None:
        target, _, mode = restore_plan
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
        "manifest": manifest,
        "restored_db_path": restored_db_path,
        "env_written": env_written,
    }
