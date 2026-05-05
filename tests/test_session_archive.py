"""Tests for versioned datasight session archives."""

from __future__ import annotations

import json
import zipfile
from io import BytesIO
from pathlib import Path

import pytest
from click.testing import CliRunner

from datasight.cli import cli
from datasight.session_archive import (
    ARCHIVE_VERSION,
    build_session_archive,
    import_session_archive,
    read_session_archive,
    validate_session_archive_id,
    write_session_archive,
)


def _sample_conversation(*, with_dashboard: bool = True) -> dict[str, object]:
    convo: dict[str, object] = {
        "title": "Monthly generation review",
        "messages": [
            {"role": "user", "content": "Summarize generation by fuel."},
            {"role": "assistant", "content": "Gas led the month."},
        ],
        "events": [
            {"event": "user_message", "data": {"text": "Summarize generation by fuel."}},
            {
                "event": "tool_start",
                "data": {
                    "tool": "run_sql",
                    "input": {
                        "sql": "SELECT energy_source_code, SUM(net_generation_mwh) FROM generation_fuel"
                    },
                },
            },
            {"event": "assistant_message", "data": {"text": "Gas led the month."}},
        ],
    }
    if with_dashboard:
        convo["dashboard"] = _sample_dashboard()
    return convo


def _sample_dashboard() -> dict[str, object]:
    return {
        "items": [
            {
                "id": 1,
                "type": "chart",
                "title": "Generation by fuel",
                "sql": "SELECT energy_source_code, SUM(net_generation_mwh) FROM generation_fuel",
                "plotly_spec": {
                    "data": [{"type": "bar", "x": ["NG"], "y": [1200]}],
                    "layout": {"title": "Generation by fuel"},
                },
            }
        ],
        "columns": 2,
        "filters": [{"id": 1, "column": "energy_source_code", "operator": "eq", "value": "NG"}],
        "title": "Fuel dashboard",
    }


def test_build_and_read_round_trip() -> None:
    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
    )

    payload = read_session_archive(archive)
    assert payload["session_id"] == "fuel-review"
    assert payload["manifest"]["archive_version"] == ARCHIVE_VERSION
    assert payload["conversation"]["title"] == "Monthly generation review"
    # Dashboard travels embedded in the conversation; no separate top-level entry.
    assert payload["dashboard"]["columns"] == 2
    assert payload["conversation"]["dashboard"]["filters"][0]["value"] == "NG"


def test_archive_layout_is_minimal() -> None:
    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
    )
    with zipfile.ZipFile(BytesIO(archive)) as zf:
        names = set(zf.namelist())
    assert names == {"manifest.json", "session/conversation.json"}


def test_dashboard_override_replaces_conversation_dashboard() -> None:
    override = {"items": [], "columns": 0, "filters": [], "title": "Override"}
    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        dashboard=override,
    )
    payload = read_session_archive(archive)
    assert payload["conversation"]["dashboard"] == override


def test_archive_never_contains_env_or_secrets(tmp_path: Path) -> None:
    convo = _sample_conversation()
    # Anything in the user's project that might hold secrets must not
    # find its way into a shareable archive.
    archive = build_session_archive(session_id="fuel-review", conversation=convo)
    with zipfile.ZipFile(BytesIO(archive)) as zf:
        names = set(zf.namelist())
        all_bytes = b"".join(zf.read(name) for name in names)

    assert ".env" not in names
    assert b"ANTHROPIC_API_KEY" not in all_bytes


def test_import_writes_conversation_with_embedded_dashboard(tmp_path: Path) -> None:
    target_dir = tmp_path / "target"
    target_dir.mkdir()

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
    )

    result = import_session_archive(archive=archive, project_dir=str(target_dir))
    assert result["session_id"] == "fuel-review"

    conversation_path = target_dir / ".datasight" / "conversations" / "fuel-review.json"
    conversation = json.loads(conversation_path.read_text(encoding="utf-8"))
    assert conversation["dashboard"]["title"] == "Fuel dashboard"
    assert conversation["events"][1]["data"]["tool"] == "run_sql"


def test_import_does_not_touch_active_dashboard_or_env(tmp_path: Path) -> None:
    target_dir = tmp_path / "target"
    target_dir.mkdir()

    existing_dashboard = {
        "items": [
            {"id": 9, "type": "chart", "title": "Mine", "sql": "SELECT 1", "plotly_spec": {}}
        ],
        "columns": 3,
        "filters": [],
        "title": "My active dashboard",
    }
    (target_dir / ".datasight").mkdir()
    (target_dir / ".datasight" / "dashboard.json").write_text(
        json.dumps(existing_dashboard), encoding="utf-8"
    )
    existing_env = "DB_MODE=duckdb\nDB_PATH=database.duckdb\nANTHROPIC_API_KEY=secret\n"
    (target_dir / ".env").write_text(existing_env, encoding="utf-8")

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
    )
    import_session_archive(archive=archive, project_dir=str(target_dir))

    assert (
        json.loads((target_dir / ".datasight" / "dashboard.json").read_text(encoding="utf-8"))
        == existing_dashboard
    )
    assert (target_dir / ".env").read_text(encoding="utf-8") == existing_env


def test_import_refuses_existing_session_without_overwrite(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / ".datasight" / "conversations").mkdir(parents=True)
    (project_dir / ".datasight" / "conversations" / "fuel-review.json").write_text(
        "{}", encoding="utf-8"
    )

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
    )
    with pytest.raises(ValueError, match="already exists"):
        import_session_archive(archive=archive, project_dir=str(project_dir))


def test_import_overwrite_replaces_existing_session(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    conv_dir = project_dir / ".datasight" / "conversations"
    conv_dir.mkdir(parents=True)
    (conv_dir / "fuel-review.json").write_text(
        json.dumps({"title": "Old", "events": [], "messages": []}), encoding="utf-8"
    )

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
    )
    import_session_archive(archive=archive, project_dir=str(project_dir), overwrite=True)
    final = json.loads((conv_dir / "fuel-review.json").read_text(encoding="utf-8"))
    assert final["title"] == "Monthly generation review"


def test_write_session_archive_streams_to_disk(tmp_path: Path) -> None:
    out_path = tmp_path / "out.zip"
    final = write_session_archive(
        output=out_path,
        session_id="fuel-review",
        conversation=_sample_conversation(),
    )
    assert final == out_path
    assert out_path.exists()
    # No leftover .part-* siblings on success.
    assert not list(tmp_path.glob("out.zip.part-*"))

    payload = read_session_archive(out_path)
    assert payload["session_id"] == "fuel-review"


def test_read_archive_accepts_path(tmp_path: Path) -> None:
    out_path = tmp_path / "session.zip"
    write_session_archive(
        output=out_path,
        session_id="fuel-review",
        conversation=_sample_conversation(),
    )
    payload = read_session_archive(out_path)
    assert payload["session_id"] == "fuel-review"


def test_read_archive_rejects_bad_zip(tmp_path: Path) -> None:
    bogus = tmp_path / "garbage.zip"
    bogus.write_bytes(b"not a zip at all")
    with pytest.raises(ValueError, match="valid zip archive"):
        read_session_archive(bogus)


def test_read_archive_rejects_unknown_format() -> None:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("manifest.json", json.dumps({"format": "something-else"}))
    with pytest.raises(ValueError, match="datasight session archive"):
        read_session_archive(buf.getvalue())


def test_read_archive_rejects_non_object_manifest() -> None:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("manifest.json", json.dumps(["this", "is", "an", "array"]))
    with pytest.raises(ValueError, match="manifest.json must be a JSON object"):
        read_session_archive(buf.getvalue())


def test_read_archive_rejects_future_major_version() -> None:
    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
    )
    mutated = BytesIO()
    with (
        zipfile.ZipFile(BytesIO(archive)) as src,
        zipfile.ZipFile(mutated, "w") as dst,
    ):
        for name in src.namelist():
            if name == "manifest.json":
                manifest = json.loads(src.read(name))
                manifest["archive_version"] = ARCHIVE_VERSION + 1
                dst.writestr(name, json.dumps(manifest))
            else:
                dst.writestr(name, src.read(name))
    with pytest.raises(ValueError, match="Unsupported session archive version"):
        read_session_archive(mutated.getvalue())


def test_validate_session_archive_id_rejects_dotted_ids() -> None:
    with pytest.raises(ValueError, match="Invalid session ID"):
        validate_session_archive_id("fuel.review")


def _write_project(project_dir: Path, *, session_id: str) -> None:
    conv_dir = project_dir / ".datasight" / "conversations"
    conv_dir.mkdir(parents=True)
    convo = _sample_conversation()
    (conv_dir / f"{session_id}.json").write_text(json.dumps(convo), encoding="utf-8")


def test_cli_session_export_import_round_trip(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    _write_project(source_dir, session_id="fuel-review")

    archive_path = tmp_path / "fuel-review.zip"
    runner = CliRunner()

    export = runner.invoke(
        cli,
        [
            "session",
            "export",
            "fuel-review",
            "--project-dir",
            str(source_dir),
            "--output-path",
            str(archive_path),
        ],
    )
    assert export.exit_code == 0, export.output
    assert archive_path.exists()

    imp = runner.invoke(
        cli,
        ["session", "import", str(archive_path), "--project-dir", str(target_dir)],
    )
    assert imp.exit_code == 0, imp.output

    imported = json.loads(
        (target_dir / ".datasight" / "conversations" / "fuel-review.json").read_text(
            encoding="utf-8"
        )
    )
    assert imported["title"] == "Monthly generation review"
    assert imported["dashboard"]["filters"][0]["value"] == "NG"
    # No active dashboard.json should be created by import.
    assert not (target_dir / ".datasight" / "dashboard.json").exists()


def test_cli_session_export_defaults_filename_to_zip(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    _write_project(project_dir, session_id="fuel-review-with-a-long-id")

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=str(tmp_path)):
        result = runner.invoke(
            cli,
            ["session", "export", "fuel-review-with-a-long-id", "--project-dir", str(project_dir)],
        )
        assert result.exit_code == 0, result.output
        assert Path("fuel-review-with-a-long-id.zip").exists()


def test_cli_session_list_warns_about_unreadable_files(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    conv_dir = project_dir / ".datasight" / "conversations"
    conv_dir.mkdir(parents=True)
    (conv_dir / "valid.json").write_text(
        json.dumps(
            {"title": "Valid", "events": [{"event": "user_message", "data": {"text": "hi"}}]}
        ),
        encoding="utf-8",
    )
    (conv_dir / "broken.json").write_text("{not json", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(cli, ["session", "list", "--project-dir", str(project_dir)])
    assert result.exit_code == 0, result.output
    assert "valid" in result.output
    assert "Warning: skipped unreadable session file broken.json" in result.output


def test_cli_session_import_requires_overwrite_for_existing_session(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
    )
    archive_path = tmp_path / "fuel-review.zip"
    archive_path.write_bytes(archive)

    existing_dir = project_dir / ".datasight" / "conversations"
    existing_dir.mkdir(parents=True)
    (existing_dir / "fuel-review.json").write_text("{}", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["session", "import", str(archive_path), "--project-dir", str(project_dir)],
    )
    assert result.exit_code != 0
    assert "already exists" in result.output
    assert "--overwrite" in result.output
