"""Tests for SparkConnectRunner.

These tests inject a fake Spark session so they can run without pyspark
or a live Spark cluster. They verify the byte-cap truncation behavior
and the cancellation-tag plumbing.
"""

from __future__ import annotations

import asyncio

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
    def __init__(self):
        self._plan = _FakePlan()


class _FakeSpark:
    def __init__(self, batches: list[pa.RecordBatch]):
        self.client = _FakeClient(batches)
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
    # Each batch is ~10k rows × ~88 bytes = roughly 880KB. With a 1MB cap,
    # we should truncate after the second batch.
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
async def test_spark_runner_interrupts_on_timeout(monkeypatch):
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
