"""Tests for SparkConnectRunner.

These tests inject a fake Spark session so they can run without pyspark
or a live Spark cluster. They verify the byte-cap truncation behavior
and the cancellation-tag plumbing.
"""

from __future__ import annotations

import asyncio

import pandas as pd
import pyarrow as pa
import pytest

from datasight.exceptions import QueryTimeoutError
from datasight.runner import SparkConnectRunner


class _FakePlan:
    pass


class _FakeClient:
    def __init__(self, batches: list[pa.RecordBatch]):
        self._batches = batches
        self.iterator_consumed = 0

    def to_table_as_iterator(self, plan, observations):
        for batch in self._batches:
            self.iterator_consumed += 1
            yield pa.Table.from_batches([batch]), None


class _FakeSparkDataFrame:
    def __init__(self, columns: list[str] | None = None):
        self._plan = _FakePlan()
        self.columns = columns or ["id", "payload"]


class _FakeCatalog:
    def __init__(self, table_names: list[str]):
        self._table_names = table_names

    def listTables(self):
        return [type("Table", (), {"name": n})() for n in self._table_names]


class _FakeConf:
    def __init__(self, values: dict[str, str] | None = None):
        self._values = values or {}

    def get(self, key: str) -> str | None:
        return self._values.get(key)


class _FakeSpark:
    def __init__(
        self,
        batches: list[pa.RecordBatch],
        table_names: list[str] | None = None,
        conf: dict[str, str] | None = None,
        version: str = "3.5.1",
    ):
        self.client = _FakeClient(batches)
        self.catalog = _FakeCatalog(table_names or [])
        self.conf = _FakeConf(conf)
        self.version = version
        self._sql_seen: list[str] = []
        self.tags_added: list[str] = []
        self.tags_removed: list[str] = []
        self.tags_interrupted: list[str] = []

    def sql(self, query: str) -> _FakeSparkDataFrame:
        self._sql_seen.append(query)
        return _FakeSparkDataFrame()

    def addTag(self, tag: str) -> None:
        self.tags_added.append(tag)

    def removeTag(self, tag: str) -> None:
        self.tags_removed.append(tag)

    def interruptTag(self, tag: str) -> None:
        self.tags_interrupted.append(tag)

    def stop(self) -> None:
        pass


def _make_batch(n_rows: int) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(
        {
            "id": list(range(n_rows)),
            # ~80 bytes per row of payload
            "payload": ["x" * 80] * n_rows,
        }
    )


@pytest.mark.asyncio
async def test_spark_runner_returns_full_result_when_under_cap():
    batches = [_make_batch(100), _make_batch(100)]
    spark = _FakeSpark(batches)
    runner = SparkConnectRunner(spark=spark, max_result_bytes=10 * 1024 * 1024)

    df = await runner.run_sql("SELECT * FROM t")

    assert len(df) == 200
    assert "truncated" not in df.attrs
    assert spark._sql_seen == ["SELECT * FROM t"]
    # Tag added then removed; never interrupted on success.
    assert spark.tags_added == spark.tags_removed
    assert spark.tags_interrupted == []


@pytest.mark.asyncio
async def test_spark_runner_truncates_at_byte_cap():
    # Each batch is ~10k rows × ~88 bytes = roughly 880KB. The cap is
    # checked *before* appending, so with a 1MB cap the first batch fits
    # (880KB) and adding the second would push total past the cap —
    # streaming halts after the first batch is accepted.
    batches = [_make_batch(10_000) for _ in range(5)]
    spark = _FakeSpark(batches)
    runner = SparkConnectRunner(spark=spark, max_result_bytes=1_000_000)

    df = await runner.run_sql("SELECT * FROM huge")

    assert df.attrs.get("truncated") is True
    assert "truncation_reason" in df.attrs
    # Should have stopped streaming early — not consumed all 5 batches.
    assert spark.client.iterator_consumed < 5
    assert len(df) < 50_000


@pytest.mark.asyncio
async def test_spark_runner_preserves_columns_for_zero_row_result():
    spark = _FakeSpark([_make_batch(0)])
    runner = SparkConnectRunner(spark=spark, max_result_bytes=10 * 1024 * 1024)

    df = await runner.run_sql("SELECT * FROM empty_t")

    assert df.empty
    assert list(df.columns) == ["id", "payload"]
    assert "truncated" not in df.attrs


@pytest.mark.asyncio
async def test_spark_runner_truncates_immediately_when_byte_cap_is_zero():
    batches = [_make_batch(10), _make_batch(10)]
    spark = _FakeSpark(batches)
    runner = SparkConnectRunner(spark=spark, max_result_bytes=0)

    df = await runner.run_sql("SELECT * FROM tiny")

    assert df.empty
    assert list(df.columns) == ["id", "payload"]
    assert df.attrs.get("truncated") is True
    assert "truncation_reason" in df.attrs
    # Cap enforced before the first non-empty batch is appended.
    assert spark.client.iterator_consumed <= 1


@pytest.mark.asyncio
async def test_spark_runner_interrupts_on_timeout():
    # Hand-rolled spark whose sql() call blocks forever so wait_for fires.
    class _BlockingSpark(_FakeSpark):
        def sql(self, query: str):
            import time

            time.sleep(10)
            return super().sql(query)

    spark = _BlockingSpark([])
    runner = SparkConnectRunner(spark=spark, query_timeout=0.1)

    with pytest.raises(QueryTimeoutError):
        await runner.run_sql("SELECT 1")

    # Give the cancellation a moment to propagate.
    await asyncio.sleep(0.05)
    assert len(spark.tags_interrupted) == 1
    assert spark.tags_interrupted[0].startswith("datasight-")


def test_log_session_info_includes_version_and_master(caplog):
    from loguru import logger as _logger

    spark = _FakeSpark(
        [],
        conf={
            "spark.master": "spark://master:7077",
            "spark.app.id": "app-20260422-0001",
            "spark.executor.instances": "8",
        },
        version="3.5.1",
    )

    sink_id = _logger.add(lambda msg: caplog.records.append(msg), level="INFO")
    try:
        SparkConnectRunner._log_session_info(spark)
    finally:
        _logger.remove(sink_id)

    combined = "\n".join(str(r) for r in caplog.records)
    assert "version" in combined and "3.5.1" in combined
    assert "spark.master" in combined and "spark://master:7077" in combined
    assert "app-20260422-0001" in combined
    # Distributed master → no local-mode warning.
    assert "running all work in one JVM" not in combined


def test_log_session_info_warns_on_local_master(caplog):
    from loguru import logger as _logger

    spark = _FakeSpark([], conf={"spark.master": "local[*]"})

    sink_id = _logger.add(lambda msg: caplog.records.append(msg), level="INFO")
    try:
        SparkConnectRunner._log_session_info(spark)
    finally:
        _logger.remove(sink_id)

    combined = "\n".join(str(r) for r in caplog.records)
    assert "local[*]" in combined
    assert "running all work in one JVM" in combined


def test_spark_runner_get_table_names_uses_catalog():
    spark = _FakeSpark([], table_names=["generation_fuel", "plants", "boilers"])
    runner = SparkConnectRunner(spark=spark)

    assert runner.get_table_names() == ["generation_fuel", "plants", "boilers"]


@pytest.mark.asyncio
async def test_spark_runner_get_row_count_returns_none():
    """Spark skips row counts so introspection never triggers a full scan."""
    runner = SparkConnectRunner(spark=_FakeSpark([]))

    assert await runner.get_row_count("any_table") is None


@pytest.mark.asyncio
async def test_introspect_schema_skips_row_count_for_spark(monkeypatch):
    """SparkConnectRunner.get_row_count wins over the generic COUNT(*) probe."""
    from datasight import schema as schema_mod

    calls: list[str] = []

    async def _fake_run_sql(sql: str):
        calls.append(sql)
        if "DESCRIBE" in sql:
            return pd.DataFrame(
                {"column_name": ["id", "payload"], "column_type": ["INT", "STRING"]}
            )
        return pd.DataFrame()

    async def _count_boom(*args, **kwargs):  # pragma: no cover - must not run
        raise AssertionError("_get_row_count must not be called for Spark runner")

    monkeypatch.setattr(schema_mod, "_get_row_count", _count_boom)

    spark = _FakeSpark([], table_names=["generation_fuel"])
    runner = SparkConnectRunner(spark=spark)

    tables = await schema_mod.introspect_schema(_fake_run_sql, runner=runner)

    assert [t.name for t in tables] == ["generation_fuel"]
    assert tables[0].row_count is None
    assert not any("COUNT(*)" in sql for sql in calls)


@pytest.mark.asyncio
async def test_agent_surfaces_truncation_in_tool_result():
    """When the runner truncates, the LLM-facing text and HTML should say so."""
    from datasight.agent import _execute_run_sql

    batches = [_make_batch(10_000) for _ in range(5)]
    spark = _FakeSpark(batches)
    # Cap sized so the first batch fits but accumulation trips before the end.
    runner = SparkConnectRunner(spark=spark, max_result_bytes=2_000_000)

    async def _run_sql(sql: str):
        return await runner.run_sql(sql)

    result = await _execute_run_sql(
        input_data={"sql": "SELECT * FROM huge"},
        run_sql=_run_sql,
        schema_map=None,
        dialect="spark",
        measure_rules=None,
        query_logger=None,
        session_id="test",
        user_question="everything please",
    )

    assert result.meta.get("truncated") is True
    assert "truncation_reason" in result.meta
    assert "Partial result" in result.result_text
    assert result.result_html is not None
    assert "sql-warning" in result.result_html


@pytest.mark.asyncio
async def test_agent_reports_truncated_empty_result_distinctly():
    """An empty result caused by truncation must not be reported as 'no rows'."""
    from datasight.agent import _execute_run_sql

    batches = [_make_batch(10)]
    spark = _FakeSpark(batches)
    runner = SparkConnectRunner(spark=spark, max_result_bytes=0)

    async def _run_sql(sql: str):
        return await runner.run_sql(sql)

    result = await _execute_run_sql(
        input_data={"sql": "SELECT * FROM anything"},
        run_sql=_run_sql,
        schema_map=None,
        dialect="spark",
        measure_rules=None,
        query_logger=None,
        session_id="test",
        user_question="everything please",
    )

    assert result.meta.get("truncated") is True
    assert "No rows returned" not in result.result_text
    assert "truncated before any rows" in result.result_text.lower()
