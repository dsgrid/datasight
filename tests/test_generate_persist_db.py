"""Tests for `datasight generate` persisting database configuration."""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest
from click.testing import CliRunner

from datasight.cli import cli
from datasight.config import set_env_vars
from datasight.explore import build_persistent_duckdb, create_ephemeral_session
from datasight.llm import TextBlock

from tests._env_helpers import DATASIGHT_ENV_VARS, scrub_datasight_env


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in DATASIGHT_ENV_VARS:
        monkeypatch.delenv(key, raising=False)
    yield
    scrub_datasight_env()


@pytest.fixture
def parquet_file(tmp_path):
    path = tmp_path / "generation_fuel.parquet"
    pd.DataFrame(
        {
            "energy_source_code": ["WND", "SUN", "NG"],
            "net_generation_mwh": [100.0, 80.0, 200.0],
        }
    ).to_parquet(path)
    return path


# ---------------------------------------------------------------------------
# build_persistent_duckdb
# ---------------------------------------------------------------------------


def test_build_persistent_duckdb_creates_views(tmp_path, parquet_file):
    _, tables_info = create_ephemeral_session([str(parquet_file)])
    db_path = tmp_path / "out.duckdb"
    result = build_persistent_duckdb(db_path, tables_info)
    assert result == db_path.resolve()
    assert db_path.exists()

    with duckdb.connect(str(db_path), read_only=True) as conn:
        view_name = tables_info[0]["name"]
        df = conn.execute(f'SELECT COUNT(*) AS n FROM "{view_name}"').fetchdf()
        assert int(df["n"].iloc[0]) == 3


def test_build_persistent_duckdb_refuses_overwrite(tmp_path, parquet_file):
    _, tables_info = create_ephemeral_session([str(parquet_file)])
    db_path = tmp_path / "out.duckdb"
    build_persistent_duckdb(db_path, tables_info)
    with pytest.raises(FileExistsError):
        build_persistent_duckdb(db_path, tables_info)
    build_persistent_duckdb(db_path, tables_info, overwrite=True)


# ---------------------------------------------------------------------------
# set_env_vars
# ---------------------------------------------------------------------------


def test_set_env_vars_creates_from_template(tmp_path):
    env_path = tmp_path / ".env"
    set_env_vars(env_path, {"DB_MODE": "duckdb", "DB_PATH": "./data.duckdb"})
    text = env_path.read_text(encoding="utf-8")
    assert "DB_MODE=duckdb" in text
    assert "DB_PATH=./data.duckdb" in text
    # Template placeholders preserved
    assert "ANTHROPIC_API_KEY" in text


def test_set_env_vars_replaces_existing_uncommented(tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text(
        "ANTHROPIC_API_KEY=my-key\nDB_MODE=sqlite\nDB_PATH=./old.sqlite\n",
        encoding="utf-8",
    )
    set_env_vars(env_path, {"DB_MODE": "duckdb", "DB_PATH": "./new.duckdb"})
    text = env_path.read_text(encoding="utf-8")
    assert "ANTHROPIC_API_KEY=my-key" in text
    assert "DB_MODE=duckdb" in text
    assert "DB_PATH=./new.duckdb" in text
    assert "./old.sqlite" not in text


def test_set_env_vars_uncomments_commented_line(tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text("# DB_PATH=./old.duckdb\n", encoding="utf-8")
    set_env_vars(env_path, {"DB_PATH": "./new.duckdb"})
    text = env_path.read_text(encoding="utf-8")
    assert "DB_PATH=./new.duckdb" in text
    assert "# DB_PATH" not in text


def test_set_env_vars_appends_missing(tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text("ANTHROPIC_API_KEY=x\n", encoding="utf-8")
    set_env_vars(env_path, {"NEW_VAR": "y"})
    text = env_path.read_text(encoding="utf-8")
    assert "ANTHROPIC_API_KEY=x" in text
    assert "NEW_VAR=y" in text


# ---------------------------------------------------------------------------
# CLI: datasight generate <file>
# ---------------------------------------------------------------------------


class _StubLLMClient:
    calls: list[dict]

    def __init__(self):
        self.calls = []

    async def create_message(self, **kwargs):
        self.calls.append(kwargs)
        text = (
            "--- schema_description.md ---\n"
            "# Schema\n\nGeneration fuel data.\n\n"
            "--- queries.yaml ---\n"
            "- question: Total MWh\n"
            "  sql: SELECT SUM(net_generation_mwh) FROM generation_fuel\n"
        )
        return SimpleNamespace(
            content=[TextBlock(text)],
            stop_reason="end_turn",
            usage=SimpleNamespace(input_tokens=1, output_tokens=1),
        )


@pytest.fixture
def stub_llm(monkeypatch):
    client = _StubLLMClient()
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setattr("datasight.cli.create_llm_client", lambda **kwargs: client)
    return client


def test_generate_creates_db_and_updates_env(tmp_path, parquet_file, stub_llm):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["generate", str(parquet_file), "--project-dir", str(tmp_path)],
    )
    assert result.exit_code == 0, result.output

    db_path = tmp_path / "database.duckdb"
    assert db_path.exists(), result.output

    env_path = tmp_path / ".env"
    assert env_path.exists()
    env_text = env_path.read_text(encoding="utf-8")
    assert "DB_MODE=duckdb" in env_text
    assert "DB_PATH=./database.duckdb" in env_text

    with duckdb.connect(str(db_path), read_only=True) as conn:
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchdf()
        assert "generation_fuel" in tables["table_name"].tolist()


def test_generate_respects_custom_db_path(tmp_path, parquet_file, stub_llm):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "generate",
            str(parquet_file),
            "--project-dir",
            str(tmp_path),
            "--db-path",
            "db/custom.duckdb",
        ],
    )
    assert result.exit_code == 0, result.output
    assert (tmp_path / "db" / "custom.duckdb").exists()
    env_text = (tmp_path / ".env").read_text(encoding="utf-8")
    assert "DB_PATH=./db/custom.duckdb" in env_text


def test_generate_sqlite_file_updates_env_without_creating_duckdb(tmp_path, stub_llm):
    db_path = tmp_path / "generation.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE generation_fuel (energy_source_code TEXT, net_generation_mwh REAL)")
    conn.executemany(
        "INSERT INTO generation_fuel VALUES (?, ?)",
        [("WND", 100.0), ("SUN", 80.0), ("NG", 200.0)],
    )
    conn.commit()
    conn.close()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["generate", str(db_path), "--project-dir", str(tmp_path)],
    )
    assert result.exit_code == 0, result.output

    assert not (tmp_path / "database.duckdb").exists()
    env_text = (tmp_path / ".env").read_text(encoding="utf-8")
    assert "DB_MODE=sqlite" in env_text
    assert "DB_PATH=./generation.sqlite" in env_text
    assert "sqlite" in stub_llm.calls[0]["messages"][0]["content"].lower()


def test_generate_duckdb_file_updates_env_without_creating_default_db(tmp_path, stub_llm):
    db_path = tmp_path / "generation.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE generation_fuel AS "
        "SELECT 'WND' AS energy_source_code, 100.0 AS net_generation_mwh"
    )
    conn.close()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["generate", str(db_path), "--project-dir", str(tmp_path)],
    )
    assert result.exit_code == 0, result.output

    env_text = (tmp_path / ".env").read_text(encoding="utf-8")
    assert "DB_MODE=duckdb" in env_text
    assert "DB_PATH=./generation.duckdb" in env_text
    assert "duckdb" in stub_llm.calls[0]["messages"][0]["content"].lower()


def test_generate_rejects_db_path_with_existing_sqlite(tmp_path, stub_llm):
    db_path = tmp_path / "generation.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE generation_fuel (energy_source_code TEXT)")
    conn.commit()
    conn.close()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "generate",
            str(db_path),
            "--project-dir",
            str(tmp_path),
            "--db-path",
            "db/custom.duckdb",
        ],
    )
    assert result.exit_code != 0
    assert "--db-path is only used" in result.output
    assert not stub_llm.calls


def test_generate_rejects_db_path_with_existing_duckdb(tmp_path, stub_llm):
    db_path = tmp_path / "generation.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE generation_fuel (energy_source_code VARCHAR)")
    conn.close()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "generate",
            str(db_path),
            "--project-dir",
            str(tmp_path),
            "--db-path",
            "db/custom.duckdb",
        ],
    )
    assert result.exit_code != 0
    assert "--db-path is only used" in result.output
    assert not stub_llm.calls


def test_generate_rejects_sqlite_file_mixed_with_other_inputs(tmp_path, parquet_file, stub_llm):
    db_path = tmp_path / "generation.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE generation_fuel (energy_source_code TEXT)")
    conn.commit()
    conn.close()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["generate", str(db_path), str(parquet_file), "--project-dir", str(tmp_path)],
    )
    assert result.exit_code != 0
    assert "SQLite input currently supports exactly one" in result.output
    assert not stub_llm.calls


def test_generate_refuses_to_overwrite_existing_db(tmp_path, parquet_file, stub_llm):
    (tmp_path / "database.duckdb").write_bytes(b"")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["generate", str(parquet_file), "--project-dir", str(tmp_path)],
    )
    assert result.exit_code != 0
    assert "already exists" in result.output


def test_generate_db_preflight_runs_before_llm_call(tmp_path, parquet_file, monkeypatch):
    """An existing DB must abort generate before docs are written or the LLM is called."""
    (tmp_path / "database.duckdb").write_bytes(b"")

    called = {"n": 0}

    def _unreachable(**kwargs):
        called["n"] += 1
        raise AssertionError("LLM should not be called when preflight rejects the run")

    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setattr("datasight.cli.create_llm_client", _unreachable)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["generate", str(parquet_file), "--project-dir", str(tmp_path)],
    )
    assert result.exit_code != 0
    assert "database.duckdb" in result.output
    assert called["n"] == 0
    # No partial docs should have been left behind.
    for name in ("schema_description.md", "queries.yaml", "measures.yaml", "time_series.yaml"):
        assert not (tmp_path / name).exists(), (
            f"{name} was written before preflight rejected the run"
        )
