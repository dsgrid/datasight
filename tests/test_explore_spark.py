"""Tests for Spark-backed file exploration.

Exercises the dispatcher in ``datasight.explore`` that routes
``inspect`` / ``generate --files`` / ``trends --files`` through Spark
Connect when the project's ``DB_MODE=spark``, and through the legacy
in-memory DuckDB session otherwise.
"""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest

from datasight.explore import (
    create_files_session_for_settings,
    create_spark_files_session,
)
from datasight.runner import EphemeralDuckDBRunner, SparkConnectRunner
from datasight.settings import DatabaseSettings


class _FakeReader:
    def __init__(self, registry: dict[str, str]):
        self._registry = registry
        self._pending_path: str | None = None

    def parquet(self, path: str):
        return _FakeSparkDF(self._registry, path, "parquet")

    def option(self, *_args, **_kwargs):
        return self

    def csv(self, path: str):
        return _FakeSparkDF(self._registry, path, "csv")


class _FakeSparkDF:
    def __init__(self, registry: dict[str, str], path: str, fmt: str):
        self._registry = registry
        self._path = path
        self._fmt = fmt

    def createOrReplaceTempView(self, name: str):
        self._registry[name] = f"{self._fmt}:{self._path}"


class _FakeSpark:
    def __init__(self):
        self.registry: dict[str, str] = {}
        self.read = _FakeReader(self.registry)

    def stop(self):
        pass


def _write_parquet(path, rows):
    import pyarrow as pa

    table = pa.table({"id": [r[0] for r in rows], "value": [r[1] for r in rows]})
    pq.write_table(table, path)


def test_create_spark_files_session_registers_temp_views(tmp_path):
    p1 = tmp_path / "generation.parquet"
    p2 = tmp_path / "plants.parquet"
    _write_parquet(p1, [(1, "wind"), (2, "solar")])
    _write_parquet(p2, [(1, "plant_a"), (2, "plant_b")])

    fake = _FakeSpark()
    runner, tables = create_spark_files_session(
        [str(p1), str(p2)],
        spark_remote="sc://ignored",
        spark=fake,
    )

    assert isinstance(runner, SparkConnectRunner)
    assert {t["name"] for t in tables} == {"generation", "plants"}
    assert set(fake.registry.keys()) == {"generation", "plants"}
    assert fake.registry["generation"].startswith("parquet:")
    assert str(p1) in fake.registry["generation"]


def test_create_spark_files_session_rejects_duckdb_files(tmp_path):
    p = tmp_path / "mydb.duckdb"
    p.write_bytes(b"\x00" * 16)  # pretend-existing duckdb file

    with pytest.raises(Exception) as excinfo:
        create_spark_files_session([str(p)], spark_remote="sc://ignored", spark=_FakeSpark())
    assert "Spark backend cannot read" in str(excinfo.value)


def test_dispatcher_spark_mode_uses_spark(tmp_path, monkeypatch):
    """DB_MODE=spark routes through create_spark_files_session."""
    p = tmp_path / "generation.parquet"
    _write_parquet(p, [(1, "wind")])

    captured = {}

    def _fake_spark_session(file_paths, *, spark_remote, spark_token, spark_max_result_bytes):
        captured["file_paths"] = file_paths
        captured["spark_remote"] = spark_remote
        captured["spark_token"] = spark_token
        captured["spark_max_result_bytes"] = spark_max_result_bytes
        return "SPARK_RUNNER", [{"name": "generation", "path": file_paths[0], "type": "parquet"}]

    monkeypatch.setattr("datasight.explore.create_spark_files_session", _fake_spark_session)

    settings = DatabaseSettings(
        mode="spark",
        spark_remote="sc://test-cluster:15002",
        spark_token="abc",
        spark_max_result_bytes=12345,
    )
    runner, tables = create_files_session_for_settings([str(p)], settings)

    assert runner == "SPARK_RUNNER"
    assert captured["spark_remote"] == "sc://test-cluster:15002"
    assert captured["spark_token"] == "abc"
    assert captured["spark_max_result_bytes"] == 12345
    assert len(tables) == 1


def test_dispatcher_duckdb_mode_uses_ephemeral(tmp_path):
    """DB_MODE=duckdb keeps the long-standing local-file behavior."""
    p = tmp_path / "generation.parquet"
    _write_parquet(p, [(1, "wind"), (2, "solar")])

    settings = DatabaseSettings(mode="duckdb")
    runner, tables = create_files_session_for_settings([str(p)], settings)

    assert isinstance(runner, EphemeralDuckDBRunner)
    assert [t["name"] for t in tables] == ["generation"]


def test_dispatcher_no_settings_uses_ephemeral(tmp_path):
    """No project / no settings → falls back to DuckDB (unchanged behavior)."""
    p = tmp_path / "generation.parquet"
    _write_parquet(p, [(1, "wind")])

    runner, tables = create_files_session_for_settings([str(p)], None)

    assert isinstance(runner, EphemeralDuckDBRunner)
    assert [t["name"] for t in tables] == ["generation"]


def test_dispatcher_logs_chosen_backend_on_every_branch(tmp_path, caplog):
    """Every dispatcher branch should log which backend was picked."""
    from loguru import logger as _logger

    p = tmp_path / "generation.parquet"
    _write_parquet(p, [(1, "wind")])

    sink_id = _logger.add(lambda msg: caplog.records.append(msg), level="INFO")
    try:
        # No settings → DuckDB (with explanation)
        create_files_session_for_settings([str(p)], None)
        # DB_MODE=duckdb → DuckDB (with mode in message)
        create_files_session_for_settings([str(p)], DatabaseSettings(mode="duckdb"))
    finally:
        _logger.remove(sink_id)

    combined = "\n".join(str(r) for r in caplog.records)
    assert "no settings" in combined.lower() or "no .env" in combined.lower()
    assert "DB_MODE=duckdb" in combined


def test_dispatcher_postgres_mode_falls_back_with_warning(tmp_path, caplog):
    """Postgres / FlightSQL can't read local files — fall back to DuckDB."""
    from loguru import logger as _logger

    p = tmp_path / "generation.parquet"
    _write_parquet(p, [(1, "wind")])

    sink_id = _logger.add(lambda msg: caplog.records.append(msg), level="WARNING")
    try:
        settings = DatabaseSettings(mode="postgres")
        runner, _ = create_files_session_for_settings([str(p)], settings)
    finally:
        _logger.remove(sink_id)

    assert isinstance(runner, EphemeralDuckDBRunner)
    combined = "\n".join(str(r) for r in caplog.records)
    assert "postgres" in combined
    assert "local DuckDB" in combined
