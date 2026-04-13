"""Tests for CachingSqlRunner."""

from __future__ import annotations

import pandas as pd
import pytest

from datasight.runner import CachingSqlRunner


class RecordingRunner:
    """Minimal SqlRunner double that records calls and returns canned frames."""

    def __init__(self, frames: dict[str, pd.DataFrame] | None = None):
        self.frames = frames or {}
        self.calls: list[str] = []

    async def run_sql(self, sql: str) -> pd.DataFrame:
        self.calls.append(sql)
        if sql in self.frames:
            return self.frames[sql].copy()
        return pd.DataFrame({"x": [1, 2, 3]})

    def close(self) -> None:
        pass

    async def __aenter__(self) -> "RecordingRunner":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


@pytest.mark.asyncio
async def test_cache_hit_avoids_second_execution():
    inner = RecordingRunner()
    runner = CachingSqlRunner(inner, max_bytes=1 << 20)

    df1 = await runner.run_sql("SELECT * FROM t")
    df2 = await runner.run_sql("SELECT * FROM t")

    assert len(inner.calls) == 1
    pd.testing.assert_frame_equal(df1, df2)
    assert runner.hits == 1
    assert runner.misses == 1


@pytest.mark.asyncio
async def test_cache_key_normalizes_whitespace_and_case():
    inner = RecordingRunner()
    runner = CachingSqlRunner(inner, max_bytes=1 << 20)

    await runner.run_sql("SELECT * FROM t")
    await runner.run_sql("  select *   from T ;  ")

    assert len(inner.calls) == 1  # second call hit the cache
    assert runner.hits == 1


@pytest.mark.asyncio
async def test_cache_returns_copy_isolating_mutations():
    inner = RecordingRunner()
    runner = CachingSqlRunner(inner, max_bytes=1 << 20)

    df1 = await runner.run_sql("SELECT 1")
    df1.loc[0, "x"] = 999
    df2 = await runner.run_sql("SELECT 1")

    assert df2.loc[0, "x"] == 1


@pytest.mark.asyncio
async def test_cache_disabled_when_max_bytes_zero():
    inner = RecordingRunner()
    runner = CachingSqlRunner(inner, max_bytes=0)

    await runner.run_sql("SELECT 1")
    await runner.run_sql("SELECT 1")

    assert len(inner.calls) == 2
    assert runner.cache_entries == 0


@pytest.mark.asyncio
async def test_oversized_result_is_not_cached():
    big = pd.DataFrame({"x": range(10_000)})
    inner = RecordingRunner({"big": big})
    # Set max smaller than the frame
    runner = CachingSqlRunner(inner, max_bytes=10)

    await runner.run_sql("big")
    await runner.run_sql("big")

    assert len(inner.calls) == 2
    assert runner.cache_entries == 0
    assert runner.cache_bytes == 0


@pytest.mark.asyncio
async def test_lru_eviction_when_over_budget():
    frames = {
        "a": pd.DataFrame({"x": range(500)}),
        "b": pd.DataFrame({"x": range(500)}),
        "c": pd.DataFrame({"x": range(500)}),
    }
    inner = RecordingRunner(frames)
    one_size = int(frames["a"].memory_usage(deep=True).sum())
    # Budget holds ~2 entries
    runner = CachingSqlRunner(inner, max_bytes=one_size * 2 + one_size // 2)

    await runner.run_sql("a")
    await runner.run_sql("b")
    await runner.run_sql("c")  # should evict "a"

    assert runner.cache_entries == 2
    # "a" now missing -> miss again
    await runner.run_sql("a")
    assert inner.calls.count("a") == 2


@pytest.mark.asyncio
async def test_clear_cache_resets_state():
    inner = RecordingRunner()
    runner = CachingSqlRunner(inner, max_bytes=1 << 20)

    await runner.run_sql("SELECT 1")
    assert runner.cache_entries == 1

    runner.clear_cache()
    assert runner.cache_entries == 0
    assert runner.cache_bytes == 0

    await runner.run_sql("SELECT 1")
    assert inner.calls.count("SELECT 1") == 2
