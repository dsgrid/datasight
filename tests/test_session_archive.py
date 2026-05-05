"""Tests for versioned datasight session archives."""

from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
import zipfile

import duckdb
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


def _make_duckdb(path: Path, *, with_row: bool = False) -> None:
    conn = duckdb.connect(str(path))
    conn.execute("CREATE TABLE generation_fuel (energy_source_code VARCHAR)")
    if with_row:
        conn.execute("INSERT INTO generation_fuel VALUES ('NG')")
    conn.close()


def test_build_and_read_round_trip(tmp_path: Path) -> None:
    project_dir = tmp_path / "source"
    project_dir.mkdir()

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(project_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
    )

    payload = read_session_archive(archive)
    assert payload["session_id"] == "fuel-review"
    assert payload["manifest"]["archive_version"] == ARCHIVE_VERSION
    assert payload["manifest"]["includes_data"] is False
    assert payload["conversation"]["title"] == "Monthly generation review"
    # Dashboard travels embedded in the conversation; no separate top-level entry.
    assert payload["dashboard"]["columns"] == 2
    assert payload["conversation"]["dashboard"]["filters"][0]["value"] == "NG"
    assert payload["source_data"]["database"] == {
        "mode": "duckdb",
        "embedded": False,
        "path": "database.duckdb",
        "path_kind": "relative",
    }


def test_archive_omits_redundant_dashboard_entry(tmp_path: Path) -> None:
    project_dir = tmp_path / "source"
    project_dir.mkdir()

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(project_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
    )
    with zipfile.ZipFile(BytesIO(archive)) as zf:
        names = set(zf.namelist())
    assert "session/conversation.json" in names
    assert "session/dashboard.json" not in names


def test_dashboard_override_replaces_conversation_dashboard(tmp_path: Path) -> None:
    project_dir = tmp_path / "source"
    project_dir.mkdir()

    override = {"items": [], "columns": 0, "filters": [], "title": "Override"}
    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        dashboard=override,
        project_dir=str(project_dir),
    )
    payload = read_session_archive(archive)
    assert payload["conversation"]["dashboard"] == override


def test_include_data_embeds_database_file(tmp_path: Path) -> None:
    project_dir = tmp_path / "source"
    project_dir.mkdir()
    _make_duckdb(project_dir / "database.duckdb")

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(project_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
        include_data=True,
    )

    payload = read_session_archive(archive)
    assert payload["manifest"]["includes_data"] is True
    db_meta = payload["source_data"]["database"]
    assert db_meta["embedded"] is True
    assert db_meta["archive_path"] == "data/database.duckdb"

    with zipfile.ZipFile(BytesIO(archive)) as zf:
        assert "data/database.duckdb" in zf.namelist()


def test_include_data_resolves_relative_path_independent_of_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_dir = tmp_path / "source"
    project_dir.mkdir()
    _make_duckdb(project_dir / "database.duckdb")

    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(project_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
        include_data=True,
    )
    assert read_session_archive(archive)["source_data"]["database"]["embedded"] is True


def test_include_data_rejects_unsupported_db_mode(tmp_path: Path) -> None:
    project_dir = tmp_path / "source"
    project_dir.mkdir()
    with pytest.raises(ValueError, match="DuckDB and SQLite"):
        build_session_archive(
            session_id="fuel-review",
            conversation=_sample_conversation(),
            project_dir=str(project_dir),
            db_mode="postgres",
            db_path="ignored",
            include_data=True,
        )


def test_import_writes_conversation_with_embedded_dashboard(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(source_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
    )

    result = import_session_archive(archive=archive, project_dir=str(target_dir))
    assert result["session_id"] == "fuel-review"
    assert result["restored_db_path"] is None
    assert result["env_written"] is False

    conversation_path = target_dir / ".datasight" / "conversations" / "fuel-review.json"
    conversation = json.loads(conversation_path.read_text(encoding="utf-8"))
    assert conversation["dashboard"]["title"] == "Fuel dashboard"
    assert conversation["events"][1]["data"]["tool"] == "run_sql"


def test_import_does_not_clobber_active_dashboard(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()

    # Pre-existing project-wide dashboard the user is editing.
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

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(source_dir),
    )
    import_session_archive(archive=archive, project_dir=str(target_dir))

    preserved = json.loads(
        (target_dir / ".datasight" / "dashboard.json").read_text(encoding="utf-8")
    )
    assert preserved == existing_dashboard


def test_import_does_not_clobber_existing_env(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()

    existing_env = (
        "DB_MODE=duckdb\nDB_PATH=existing.duckdb\nANTHROPIC_API_KEY=secret-do-not-erase\n"
    )
    (target_dir / ".env").write_text(existing_env, encoding="utf-8")
    _make_duckdb(source_dir / "database.duckdb")

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(source_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
        include_data=True,
    )
    # Allow the data file to land at database.duckdb in target (no prior file there).
    result = import_session_archive(archive=archive, project_dir=str(target_dir))

    assert (target_dir / ".env").read_text(encoding="utf-8") == existing_env
    assert result["env_written"] is False
    assert (target_dir / "database.duckdb").exists()


def test_import_writes_minimal_env_in_fresh_project(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()
    _make_duckdb(source_dir / "database.duckdb", with_row=True)

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(source_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
        include_data=True,
    )

    result = import_session_archive(archive=archive, project_dir=str(target_dir))

    assert result["env_written"] is True
    assert (target_dir / ".env").read_text(encoding="utf-8") == (
        "DB_MODE=duckdb\nDB_PATH=database.duckdb\n"
    )
    conn = duckdb.connect(str(target_dir / "database.duckdb"), read_only=True)
    rows = conn.execute("SELECT energy_source_code FROM generation_fuel").fetchall()
    conn.close()
    assert rows == [("NG",)]


def test_import_refuses_to_overwrite_existing_db_file(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()
    _make_duckdb(source_dir / "database.duckdb")

    # Drop a sentinel file at the same path the importer would restore to.
    (target_dir / "database.duckdb").write_bytes(b"existing-bytes")

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(source_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
        include_data=True,
    )

    with pytest.raises(ValueError, match="already exists"):
        import_session_archive(archive=archive, project_dir=str(target_dir))

    # The conversation file must not have been written either.
    assert not (target_dir / ".datasight" / "conversations" / "fuel-review.json").exists()
    assert (target_dir / "database.duckdb").read_bytes() == b"existing-bytes"


def test_import_overwrite_replaces_existing_db(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()
    _make_duckdb(source_dir / "database.duckdb", with_row=True)
    (target_dir / "database.duckdb").write_bytes(b"existing-bytes")

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(source_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
        include_data=True,
    )

    import_session_archive(archive=archive, project_dir=str(target_dir), overwrite=True)
    conn = duckdb.connect(str(target_dir / "database.duckdb"), read_only=True)
    rows = conn.execute("SELECT energy_source_code FROM generation_fuel").fetchall()
    conn.close()
    assert rows == [("NG",)]


def test_archive_omits_env_and_scrubs_external_db_paths(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / ".env").write_text(
        "DB_MODE=duckdb\nDB_PATH=database.duckdb\nANTHROPIC_API_KEY=top-secret\n",
        encoding="utf-8",
    )

    external_dir = tmp_path / "private" / "datasets"
    external_dir.mkdir(parents=True)
    abs_db = external_dir / "shared.duckdb"

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(project_dir),
        db_mode="duckdb",
        db_path=str(abs_db),
    )

    with zipfile.ZipFile(BytesIO(archive)) as zf:
        names = set(zf.namelist())
        manifest = json.loads(zf.read("manifest.json"))
        source_data = json.loads(zf.read("metadata/source_data.json"))
        all_bytes = b"".join(zf.read(name) for name in names)

    # The archive must not bundle any .env or echo the user's secrets.
    assert ".env" not in names
    assert b"top-secret" not in all_bytes
    assert b"ANTHROPIC_API_KEY" not in all_bytes

    # External absolute paths get reduced to a basename so the exporter's
    # filesystem layout doesn't leak.
    db = source_data["database"]
    assert db["path"] == "shared.duckdb"
    assert db["path_kind"] == "external"
    assert str(external_dir) not in json.dumps(manifest) + json.dumps(source_data)


def test_import_restores_external_db_under_basename_in_project(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    external_dir = tmp_path / "external"
    source_dir.mkdir()
    target_dir.mkdir()
    external_dir.mkdir()
    db_path = external_dir / "shared.duckdb"
    _make_duckdb(db_path)

    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(source_dir),
        db_mode="duckdb",
        db_path=str(db_path),
        include_data=True,
    )
    import_session_archive(archive=archive, project_dir=str(target_dir))

    # External absolute paths get pinned to the project root by basename so
    # the exporter can't dictate filesystem layout on the importer.
    assert (target_dir / "shared.duckdb").exists()
    assert not (external_dir / "shared.duckdb").samefile(target_dir / "shared.duckdb")
    assert (target_dir / ".env").read_text(encoding="utf-8") == (
        "DB_MODE=duckdb\nDB_PATH=shared.duckdb\n"
    )


def test_import_rejects_traversal_restore_path(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    base = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(project_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
    )

    mutated = BytesIO()
    with (
        zipfile.ZipFile(BytesIO(base)) as src,
        zipfile.ZipFile(mutated, "w", compression=zipfile.ZIP_DEFLATED) as dst,
    ):
        for name in src.namelist():
            if name == "metadata/source_data.json":
                source_data = json.loads(src.read(name))
                source_data["database"] = {
                    "mode": "duckdb",
                    "embedded": True,
                    "path_kind": "relative",
                    "path": "../escape.duckdb",
                    "archive_path": "data/database.duckdb",
                }
                dst.writestr(name, json.dumps(source_data))
            else:
                dst.writestr(name, src.read(name))
        dst.writestr("data/database.duckdb", b"not-a-real-db")

    with pytest.raises(ValueError, match="traverse upward"):
        import_session_archive(archive=mutated.getvalue(), project_dir=str(project_dir))

    # No partial state should be left behind on rejection.
    assert not (project_dir / ".datasight" / "conversations" / "fuel-review.json").exists()


def test_read_archive_rejects_unknown_format(tmp_path: Path) -> None:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("manifest.json", json.dumps({"format": "something-else"}))
    with pytest.raises(ValueError, match="datasight session archive"):
        read_session_archive(buf.getvalue())


def test_read_archive_rejects_future_major_version(tmp_path: Path) -> None:
    project_dir = tmp_path / "source"
    project_dir.mkdir()
    archive = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(project_dir),
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


def test_write_session_archive_streams_to_disk(tmp_path: Path) -> None:
    project_dir = tmp_path / "source"
    project_dir.mkdir()
    _make_duckdb(project_dir / "database.duckdb", with_row=True)

    out_path = tmp_path / "out.zip"
    final = write_session_archive(
        output=out_path,
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(project_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
        include_data=True,
    )

    assert final == out_path
    assert out_path.exists()
    # No leftover .part-* siblings on success.
    assert not list(tmp_path.glob("out.zip.part-*"))

    payload = read_session_archive(out_path)
    assert payload["manifest"]["includes_data"] is True
    assert payload["source_data"]["database"]["archive_path"] == "data/database.duckdb"


def test_write_session_archive_cleans_up_partial_on_failure(tmp_path: Path) -> None:
    project_dir = tmp_path / "source"
    project_dir.mkdir()
    # No database file → include_data fails after the .part has been opened.
    out_path = tmp_path / "out.zip"
    with pytest.raises(ValueError, match="Database file not found"):
        write_session_archive(
            output=out_path,
            session_id="fuel-review",
            conversation=_sample_conversation(),
            project_dir=str(project_dir),
            db_mode="duckdb",
            db_path="database.duckdb",
            include_data=True,
        )
    assert not out_path.exists()
    assert not list(tmp_path.glob("out.zip.part-*"))


def test_read_archive_accepts_path(tmp_path: Path) -> None:
    project_dir = tmp_path / "source"
    project_dir.mkdir()
    out_path = tmp_path / "session.zip"
    write_session_archive(
        output=out_path,
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(project_dir),
    )
    payload = read_session_archive(out_path)
    assert payload["session_id"] == "fuel-review"


def test_read_archive_rejects_bad_zip(tmp_path: Path) -> None:
    bogus = tmp_path / "garbage.zip"
    bogus.write_bytes(b"not a zip at all")
    with pytest.raises(ValueError, match="valid zip archive"):
        read_session_archive(bogus)


def test_read_archive_rejects_non_object_manifest(tmp_path: Path) -> None:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("manifest.json", json.dumps(["this", "is", "an", "array"]))
    with pytest.raises(ValueError, match="manifest.json must be a JSON object"):
        read_session_archive(buf.getvalue())


def test_import_rejects_symlink_escape(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    # ``project_dir/data`` is a symlink to a sibling outside the project.
    (project_dir / "data").symlink_to(outside_dir, target_is_directory=True)

    _make_duckdb(tmp_path / "src.duckdb")
    base = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(project_dir),
        db_mode="duckdb",
        db_path=str(tmp_path / "src.duckdb"),
        include_data=True,
    )

    # Mutate the metadata to claim the embedded DB belongs at data/x.duckdb,
    # which would resolve through the symlink and escape the project tree.
    mutated = BytesIO()
    with (
        zipfile.ZipFile(BytesIO(base)) as src,
        zipfile.ZipFile(mutated, "w", compression=zipfile.ZIP_DEFLATED) as dst,
    ):
        for name in src.namelist():
            if name == "metadata/source_data.json":
                source_data = json.loads(src.read(name))
                source_data["database"]["path"] = "data/x.duckdb"
                source_data["database"]["path_kind"] = "relative"
                dst.writestr(name, json.dumps(source_data))
            else:
                dst.writestr(name, src.read(name))

    with pytest.raises(ValueError, match="escapes the project directory"):
        import_session_archive(archive=mutated.getvalue(), project_dir=str(project_dir))
    # No conversation file written on rejection.
    assert not (project_dir / ".datasight" / "conversations" / "fuel-review.json").exists()
    # No file extracted into the symlinked-out location.
    assert not list(outside_dir.iterdir())


def test_import_rolls_back_on_missing_archive_member(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    base = build_session_archive(
        session_id="fuel-review",
        conversation=_sample_conversation(),
        project_dir=str(project_dir),
        db_mode="duckdb",
        db_path="database.duckdb",
    )

    # Mark the archive as embedding a data file that doesn't actually exist
    # in the zip — the import must reject before writing the conversation.
    mutated = BytesIO()
    with (
        zipfile.ZipFile(BytesIO(base)) as src,
        zipfile.ZipFile(mutated, "w", compression=zipfile.ZIP_DEFLATED) as dst,
    ):
        for name in src.namelist():
            if name == "metadata/source_data.json":
                source_data = json.loads(src.read(name))
                source_data["database"] = {
                    "mode": "duckdb",
                    "embedded": True,
                    "path": "database.duckdb",
                    "path_kind": "relative",
                    "archive_path": "data/database.duckdb",
                }
                dst.writestr(name, json.dumps(source_data))
            else:
                dst.writestr(name, src.read(name))

    with pytest.raises(ValueError, match="missing embedded data file"):
        import_session_archive(archive=mutated.getvalue(), project_dir=str(project_dir))

    assert not (project_dir / ".datasight" / "conversations" / "fuel-review.json").exists()
    assert not (project_dir / "database.duckdb").exists()
    # No leftover .part-* files.
    assert (
        not list((project_dir / ".datasight" / "conversations").glob("*.part-*"))
        if (project_dir / ".datasight" / "conversations").exists()
        else True
    )


def test_validate_session_archive_id_rejects_dotted_ids() -> None:
    with pytest.raises(ValueError, match="Invalid session ID"):
        validate_session_archive_id("fuel.review")


def _write_project(project_dir: Path, *, session_id: str, with_db: bool) -> None:
    conv_dir = project_dir / ".datasight" / "conversations"
    conv_dir.mkdir(parents=True)
    if with_db:
        _make_duckdb(project_dir / "database.duckdb", with_row=True)
        (project_dir / ".env").write_text(
            "DB_MODE=duckdb\nDB_PATH=database.duckdb\n", encoding="utf-8"
        )
    convo = _sample_conversation()
    (conv_dir / f"{session_id}.json").write_text(json.dumps(convo), encoding="utf-8")


def test_cli_session_export_import_round_trip(tmp_path: Path, monkeypatch) -> None:
    for key in ("DB_PATH", "DB_MODE", "DATASIGHT_PROJECT"):
        monkeypatch.delenv(key, raising=False)

    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    _write_project(source_dir, session_id="fuel-review", with_db=True)

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


def test_cli_session_export_include_data_round_trip(tmp_path: Path, monkeypatch) -> None:
    for key in ("DB_PATH", "DB_MODE", "DATASIGHT_PROJECT"):
        monkeypatch.delenv(key, raising=False)

    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    _write_project(source_dir, session_id="fuel-review", with_db=True)

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
            "--include-data",
        ],
    )
    assert export.exit_code == 0, export.output

    imp = runner.invoke(
        cli,
        ["session", "import", str(archive_path), "--project-dir", str(target_dir)],
    )
    assert imp.exit_code == 0, imp.output
    assert (target_dir / ".env").read_text(encoding="utf-8") == (
        "DB_MODE=duckdb\nDB_PATH=database.duckdb\n"
    )
    conn = duckdb.connect(str(target_dir / "database.duckdb"), read_only=True)
    rows = conn.execute("SELECT energy_source_code FROM generation_fuel").fetchall()
    conn.close()
    assert rows == [("NG",)]


def test_cli_session_export_omits_db_metadata_for_project_without_env(
    tmp_path: Path, monkeypatch
) -> None:
    for key in ("DB_PATH", "DB_MODE", "DATASIGHT_PROJECT"):
        monkeypatch.delenv(key, raising=False)

    project_dir = tmp_path / "project"
    _write_project(project_dir, session_id="fuel-review", with_db=False)
    assert not (project_dir / ".env").exists()

    archive_path = tmp_path / "out.zip"
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
                "--output-path",
                str(archive_path),
            ],
        )
        assert result.exit_code == 0, result.output

    payload = read_session_archive(archive_path)
    # A bare Settings.from_env() would have stamped DuckDB defaults into
    # source_data; with no .env we should record no database metadata.
    assert payload["source_data"] == {}


def test_cli_session_export_include_data_without_env_errors(tmp_path: Path, monkeypatch) -> None:
    for key in ("DB_PATH", "DB_MODE", "DATASIGHT_PROJECT"):
        monkeypatch.delenv(key, raising=False)

    project_dir = tmp_path / "project"
    _write_project(project_dir, session_id="fuel-review", with_db=False)

    archive_path = tmp_path / "out.zip"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "session",
            "export",
            "fuel-review",
            "--project-dir",
            str(project_dir),
            "--output-path",
            str(archive_path),
            "--include-data",
        ],
    )
    assert result.exit_code != 0
    assert "requires a configured database" in result.output


def test_cli_session_export_defaults_filename_to_zip(tmp_path: Path, monkeypatch) -> None:
    for key in ("DB_PATH", "DB_MODE", "DATASIGHT_PROJECT"):
        monkeypatch.delenv(key, raising=False)

    project_dir = tmp_path / "project"
    _write_project(project_dir, session_id="fuel-review-with-a-long-id", with_db=True)

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
        ["session", "import", str(archive_path), "--project-dir", str(project_dir)],
    )
    assert result.exit_code != 0
    assert "already exists" in result.output
    assert "--overwrite" in result.output
