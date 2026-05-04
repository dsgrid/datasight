"""Tests for versioned datasight session archives."""

from __future__ import annotations

import json
from pathlib import Path
import zipfile

import duckdb
from click.testing import CliRunner

from datasight.cli import cli
from datasight.session_archive import (
    SESSION_ARCHIVE_VERSION,
    build_session_archive,
    import_session_archive,
    read_session_archive,
)


def _sample_conversation() -> dict[str, object]:
    return {
        "title": "Monthly generation review",
        "messages": [
            {"role": "user", "content": "Summarize generation by fuel."},
            {"role": "assistant", "content": "Gas led the month."},
        ],
        "events": [
            {
                "event": "user_message",
                "data": {"text": "Summarize generation by fuel."},
            },
            {
                "event": "tool_start",
                "data": {
                    "tool": "run_sql",
                    "input": {
                        "sql": "SELECT energy_source_code, SUM(net_generation_mwh) FROM generation_fuel"
                    },
                },
            },
            {
                "event": "assistant_message",
                "data": {"text": "Gas led the month."},
            },
        ],
    }


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
        "filters": [
            {
                "id": 1,
                "column": "energy_source_code",
                "operator": "eq",
                "value": "NG",
            }
        ],
        "title": "Fuel dashboard",
    }


def test_build_and_read_session_archive_round_trip(tmp_path: Path) -> None:
    project_dir = tmp_path / "source"
    project_dir.mkdir()
    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        dashboard=_sample_dashboard(),
        project_dir=str(project_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
    )

    payload = read_session_archive(archive)
    assert payload["session_id"] == "fuel-review"
    assert payload["manifest"]["archive_version"] == SESSION_ARCHIVE_VERSION
    assert payload["conversation"]["title"] == "Monthly generation review"
    assert payload["dashboard"]["columns"] == 2
    assert payload["dashboard"]["filters"][0]["value"] == "NG"
    assert payload["source_data"]["database"]["embedded"] is False


def test_archive_includes_relative_source_data_reference(tmp_path: Path) -> None:
    project_dir = tmp_path / "source"
    project_dir.mkdir()
    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        dashboard=_sample_dashboard(),
        project_dir=str(project_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
    )

    archive_path = tmp_path / "session.dszip"
    archive_path.write_bytes(archive)
    with zipfile.ZipFile(archive_path) as zf:
        source_data = json.loads(zf.read("metadata/source_data.json"))
    assert source_data["database"]["mode"] == "duckdb"
    assert source_data["database"]["path"] == "database.duckdb"
    assert source_data["database"]["path_kind"] == "relative"


def test_archive_include_data_embeds_database_file(tmp_path: Path) -> None:
    project_dir = tmp_path / "source"
    project_dir.mkdir()
    db_path = project_dir / "database.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE generation_fuel (energy_source_code VARCHAR)")
    conn.close()

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        dashboard=_sample_dashboard(),
        project_dir=str(project_dir),
        db_mode="duckdb",
        db_path=str(db_path),
        include_data=True,
    )

    payload = read_session_archive(archive)
    assert payload["manifest"]["includes_data"] is True
    assert payload["source_data"]["database"]["embedded"] is True
    assert payload["source_data"]["database"]["archive_path"] == "data/database.duckdb"

    archive_path = tmp_path / "session.zip"
    archive_path.write_bytes(archive)
    with zipfile.ZipFile(archive_path) as zf:
        assert "data/database.duckdb" in zf.namelist()


def test_import_session_archive_writes_conversation_and_dashboard(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        dashboard=_sample_dashboard(),
        project_dir=str(source_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
    )

    result = import_session_archive(
        archive=archive,
        project_dir=str(target_dir),
    )
    assert result["session_id"] == "fuel-review"

    conversation_path = target_dir / ".datasight" / "conversations" / "fuel-review.json"
    dashboard_path = target_dir / ".datasight" / "dashboard.json"
    conversation = json.loads(conversation_path.read_text(encoding="utf-8"))
    dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))

    assert conversation["dashboard"]["title"] == "Fuel dashboard"
    assert conversation["events"][1]["data"]["tool"] == "run_sql"
    assert dashboard["items"][0]["title"] == "Generation by fuel"


def test_import_session_archive_restores_embedded_database_and_env(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()

    db_path = source_dir / "database.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE generation_fuel (energy_source_code VARCHAR)")
    conn.execute("INSERT INTO generation_fuel VALUES ('NG')")
    conn.close()

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        dashboard=_sample_dashboard(),
        project_dir=str(source_dir),
        db_mode="duckdb",
        db_path=str(db_path),
        include_data=True,
    )

    result = import_session_archive(
        archive=archive,
        project_dir=str(target_dir),
    )

    restored_db_path = target_dir / "database.duckdb"
    assert restored_db_path in result["restored_paths"]
    assert restored_db_path.exists()
    assert (target_dir / ".env").read_text(encoding="utf-8") == (
        "DB_MODE=duckdb\nDB_PATH=database.duckdb\n"
    )

    conn = duckdb.connect(str(restored_db_path), read_only=True)
    rows = conn.execute("SELECT energy_source_code FROM generation_fuel").fetchall()
    conn.close()
    assert rows == [("NG",)]


def test_cli_session_export_import_round_trip(tmp_path: Path, monkeypatch) -> None:
    for key in ("DB_PATH", "DB_MODE", "DATASIGHT_PROJECT"):
        monkeypatch.delenv(key, raising=False)

    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    conv_dir = source_dir / ".datasight" / "conversations"
    conv_dir.mkdir(parents=True)
    target_dir.mkdir()

    db_path = source_dir / "database.duckdb"
    duckdb.connect(str(db_path)).close()
    (source_dir / ".env").write_text("DB_MODE=duckdb\nDB_PATH=database.duckdb\n", encoding="utf-8")

    conversation = _sample_conversation()
    conversation["dashboard"] = _sample_dashboard()
    (conv_dir / "fuel-review.json").write_text(json.dumps(conversation), encoding="utf-8")

    archive_path = tmp_path / "fuel-review.zip"
    runner = CliRunner()

    export_result = runner.invoke(
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
    assert export_result.exit_code == 0, export_result.output
    assert archive_path.exists()

    import_result = runner.invoke(
        cli,
        [
            "session",
            "import",
            str(archive_path),
            "--project-dir",
            str(target_dir),
        ],
    )
    assert import_result.exit_code == 0, import_result.output

    imported = json.loads(
        (target_dir / ".datasight" / "conversations" / "fuel-review.json").read_text(
            encoding="utf-8"
        )
    )
    active_dashboard = json.loads(
        (target_dir / ".datasight" / "dashboard.json").read_text(encoding="utf-8")
    )

    assert imported["title"] == "Monthly generation review"
    assert imported["dashboard"]["filters"][0]["value"] == "NG"
    assert active_dashboard["title"] == "Fuel dashboard"


def test_cli_session_export_include_data_round_trip_restores_db(
    tmp_path: Path, monkeypatch
) -> None:
    for key in ("DB_PATH", "DB_MODE", "DATASIGHT_PROJECT"):
        monkeypatch.delenv(key, raising=False)

    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    conv_dir = source_dir / ".datasight" / "conversations"
    conv_dir.mkdir(parents=True)
    target_dir.mkdir()

    db_path = source_dir / "database.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE generation_fuel (energy_source_code VARCHAR)")
    conn.execute("INSERT INTO generation_fuel VALUES ('NG')")
    conn.close()
    (source_dir / ".env").write_text("DB_MODE=duckdb\nDB_PATH=database.duckdb\n", encoding="utf-8")

    conversation = _sample_conversation()
    conversation["dashboard"] = _sample_dashboard()
    (conv_dir / "fuel-review.json").write_text(json.dumps(conversation), encoding="utf-8")

    archive_path = tmp_path / "fuel-review.zip"
    runner = CliRunner()
    export_result = runner.invoke(
        cli,
        [
            "session",
            "export",
            "fuel-review",
            "--project-dir",
            str(source_dir),
            "--output-path",
            str(archive_path),
            "--include-data",
        ],
    )
    assert export_result.exit_code == 0, export_result.output

    import_result = runner.invoke(
        cli,
        [
            "session",
            "import",
            str(archive_path),
            "--project-dir",
            str(target_dir),
        ],
    )
    assert import_result.exit_code == 0, import_result.output
    assert (target_dir / ".env").read_text(encoding="utf-8") == (
        "DB_MODE=duckdb\nDB_PATH=database.duckdb\n"
    )

    conn = duckdb.connect(str(target_dir / "database.duckdb"), read_only=True)
    rows = conn.execute("SELECT energy_source_code FROM generation_fuel").fetchall()
    conn.close()
    assert rows == [("NG",)]


def test_cli_session_export_defaults_to_zip_filename(tmp_path: Path, monkeypatch) -> None:
    for key in ("DB_PATH", "DB_MODE", "DATASIGHT_PROJECT"):
        monkeypatch.delenv(key, raising=False)

    project_dir = tmp_path / "project"
    conv_dir = project_dir / ".datasight" / "conversations"
    conv_dir.mkdir(parents=True)

    db_path = project_dir / "database.duckdb"
    duckdb.connect(str(db_path)).close()
    (project_dir / ".env").write_text(
        "DB_MODE=duckdb\nDB_PATH=database.duckdb\n", encoding="utf-8"
    )

    conversation = _sample_conversation()
    conversation["dashboard"] = _sample_dashboard()
    (conv_dir / "fuel-review.json").write_text(json.dumps(conversation), encoding="utf-8")

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=str(tmp_path)):
        result = runner.invoke(
            cli,
            [
                "session",
                "export",
                "fuel-review",
                "--project-dir",
                str(project_dir),
            ],
        )
        assert result.exit_code == 0, result.output
        assert Path("fuel-review.zip").exists()


def test_cli_session_import_requires_overwrite_for_existing_session(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        dashboard=_sample_dashboard(),
        project_dir=str(project_dir),
    )
    archive_path = tmp_path / "fuel-review.zip"
    archive_path.write_bytes(archive)

    existing_dir = project_dir / ".datasight" / "conversations"
    existing_dir.mkdir(parents=True)
    (existing_dir / "fuel-review.json").write_text("{}", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "session",
            "import",
            str(archive_path),
            "--project-dir",
            str(project_dir),
        ],
    )
    assert result.exit_code != 0
    assert "already exists" in result.output
    assert "--overwrite" in result.output
