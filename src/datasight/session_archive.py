"""Versioned session archive helpers.

A session archive is a zip file with this layout::

    manifest.json              # format/version/exported_at/session_id/title
    session/conversation.json  # the .datasight/conversations/<id>.json file,
                               # which already embeds the per-session dashboard
                               # under conversation["dashboard"]

The on-disk shape of conversations and per-session dashboards is the
authoritative source of truth (see ``ConversationStore`` and
``DashboardStore`` in ``datasight.web.app``). This module just hands those
files in and out of a zip with a manifest.

Privacy
-------
Archives are intended to be shared with collaborators. They never include
``.env`` and never read LLM credentials. Sharing the underlying database
is out of scope — wrap the project directory yourself if a recipient
needs runnable data.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import uuid
import zipfile
from collections.abc import Iterator
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import IO, Any

from datasight import __version__

ARCHIVE_FORMAT = "datasight-session-archive"
ARCHIVE_VERSION = 1
_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")

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


def _build_manifest(*, session_id: str, title: str) -> dict[str, Any]:
    return {
        "format": ARCHIVE_FORMAT,
        "archive_version": ARCHIVE_VERSION,
        "datasight_version": __version__,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "session_id": session_id,
        "title": title,
    }


def _write_archive_to(
    sink: IO[bytes],
    *,
    session_id: str,
    conversation: dict[str, Any],
    dashboard: dict[str, Any] | None,
) -> None:
    validate_session_archive_id(session_id)

    payload = dict(conversation)
    if dashboard is not None:
        payload["dashboard"] = dashboard

    manifest = _build_manifest(
        session_id=session_id,
        title=str(payload.get("title", "Untitled")),
    )

    with zipfile.ZipFile(sink, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest, indent=2, sort_keys=True))
        zf.writestr(
            "session/conversation.json",
            json.dumps(payload, indent=2, sort_keys=True),
        )


def build_session_archive(
    *,
    session_id: str,
    conversation: dict[str, Any],
    dashboard: dict[str, Any] | None = None,
) -> bytes:
    """Build a portable zip archive for a saved datasight session.

    Buffers the archive in memory and returns its bytes — convenient for
    callers that don't have an output path. Use
    :func:`write_session_archive` when streaming straight to disk.
    """
    buf = BytesIO()
    _write_archive_to(
        buf,
        session_id=session_id,
        conversation=conversation,
        dashboard=dashboard,
    )
    return buf.getvalue()


def write_session_archive(
    *,
    output: str | os.PathLike[str],
    session_id: str,
    conversation: dict[str, Any],
    dashboard: dict[str, Any] | None = None,
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
            )
        os.replace(tmp_path, final_path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            tmp_path.unlink()
        raise
    return final_path


@contextlib.contextmanager
def _open_archive(archive: ArchiveSource) -> Iterator[zipfile.ZipFile]:
    """Open ``archive`` (bytes or path) as a ``ZipFile``.

    ``BadZipFile`` is normalized to ``ValueError`` so the CLI surfaces
    a clean error.
    """
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


def _read_manifest_payload(zf: zipfile.ZipFile) -> tuple[dict[str, Any], dict[str, Any]]:
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

    return manifest, conversation


def read_session_archive(archive: ArchiveSource) -> dict[str, Any]:
    """Read and validate a datasight session archive without extracting it.

    ``archive`` may be raw bytes or a path; path inputs stream from disk.
    """
    with _open_archive(archive) as zf:
        manifest, conversation = _read_manifest_payload(zf)

    session_id = validate_session_archive_id(str(manifest.get("session_id") or ""))
    dashboard = conversation.get("dashboard")
    if not isinstance(dashboard, dict):
        dashboard = {}
    return {
        "manifest": manifest,
        "session_id": session_id,
        "conversation": conversation,
        "dashboard": dashboard,
    }


def import_session_archive(
    *,
    archive: ArchiveSource,
    project_dir: str,
    session_id: str | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Import a session archive into a datasight project directory.

    Writes ``.datasight/conversations/<session_id>.json``. The
    project-wide ``.datasight/dashboard.json`` and any existing ``.env``
    are deliberately left untouched — the per-session dashboard travels
    inside the conversation JSON, and the web app reads it from there
    via ``?session_id=...``.
    """
    project_root = Path(project_dir).resolve()

    with _open_archive(archive) as zf:
        manifest, conversation = _read_manifest_payload(zf)

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

    conversations_dir.mkdir(parents=True, exist_ok=True)
    conversation_path.write_text(
        json.dumps(conversation, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return {
        "session_id": target_session_id,
        "conversation_path": conversation_path,
        "manifest": manifest,
    }
