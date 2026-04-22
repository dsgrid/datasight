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


def test_log_session_info_times_out_on_unresponsive_server(caplog, monkeypatch):
    """A hung gRPC call must not freeze the CLI — probe should time out."""
    import queue as _queue

    from loguru import logger as _logger

    # Shrink the wait inside the runner so the test takes ms, not 30s. The
    # daemon thread itself is left to be cleaned up by interpreter exit
    # because that's exactly the production behavior we want to prove.
    real_get = _queue.Queue.get

    def _quick_get(self, *args, **kwargs):
        kwargs["timeout"] = 0.2
        return real_get(self, *args, **kwargs)

    monkeypatch.setattr(_queue.Queue, "get", _quick_get)

    class _HangingConf:
        def get(self, _key):
            import time as _time

            _time.sleep(5)
            return "never"

    spark = _FakeSpark([])
    spark.conf = _HangingConf()

    sink_id = _logger.add(lambda msg: caplog.records.append(msg), level="INFO")
    try:
        SparkConnectRunner._log_session_info(spark)
    finally:
        _logger.remove(sink_id)

    combined = "\n".join(str(r) for r in caplog.records)
    assert "timed out" in combined


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


def test_enable_ansi_quoted_identifiers_sets_session_config():
    """Spark must be told to treat "name" as an identifier, not a string."""

    class _RecordingConf:
        def __init__(self):
            self.values: dict[str, str] = {}

        def set(self, key, value):
            self.values[key] = value

        def get(self, key):
            return self.values.get(key)

    spark = _FakeSpark([])
    spark.conf = _RecordingConf()

    SparkConnectRunner._enable_ansi_quoted_identifiers(spark)

    assert spark.conf.values.get("spark.sql.ansi.doubleQuotedIdentifiers") == "true"


def test_enable_ansi_quoted_identifiers_warns_but_does_not_raise(caplog):
    """If the conf can't be set, log a warning but keep the connection alive."""
    from loguru import logger as _logger

    class _RejectingConf:
        def set(self, *_args, **_kwargs):
            raise RuntimeError("config locked by admin")

    spark = _FakeSpark([])
    spark.conf = _RejectingConf()

    sink_id = _logger.add(lambda msg: caplog.records.append(msg), level="WARNING")
    try:
        # Must not raise even if conf.set blows up.
        SparkConnectRunner._enable_ansi_quoted_identifiers(spark)
    finally:
        _logger.remove(sink_id)

    combined = "\n".join(str(r) for r in caplog.records)
    assert "doubleQuotedIdentifiers" in combined
    assert "config locked by admin" in combined


def test_probe_connection_raises_quickly_when_server_unreachable(monkeypatch):
    """An unresponsive server should fail in seconds, not hang for minutes."""
    import queue as _queue

    real_get = _queue.Queue.get

    def _quick_get(self, *args, **kwargs):
        kwargs["timeout"] = 0.2
        return real_get(self, *args, **kwargs)

    monkeypatch.setattr(_queue.Queue, "get", _quick_get)

    class _HangingSpark:
        @property
        def version(self):
            import time as _time

            _time.sleep(5)  # simulate a hung gRPC call

    from datasight.exceptions import ConnectionError as _ConnErr

    with pytest.raises(_ConnErr) as excinfo:
        SparkConnectRunner._probe_connection(_HangingSpark(), "sc://nowhere:15002", timeout=999.0)
    assert "Could not reach Spark Connect" in str(excinfo.value)
    assert "sc://nowhere:15002" in str(excinfo.value)


def test_probe_connection_propagates_handshake_error():
    """When spark.version raises (e.g. auth failure) we surface it cleanly."""

    class _RejectingSpark:
        @property
        def version(self):
            raise RuntimeError("UNAUTHENTICATED: invalid token")

    from datasight.exceptions import ConnectionError as _ConnErr

    with pytest.raises(_ConnErr) as excinfo:
        SparkConnectRunner._probe_connection(_RejectingSpark(), "sc://x:15002")
    assert "rejected the handshake" in str(excinfo.value)
    assert "UNAUTHENTICATED" in str(excinfo.value)


def test_spark_runner_get_columns_uses_catalog_api():
    """Use spark.catalog.listColumns rather than SQL DESCRIBE/information_schema."""
    from types import SimpleNamespace

    class _SparkWithColumns(_FakeSpark):
        def __init__(self, columns_by_table):
            super().__init__([])
            self._columns_by_table = columns_by_table

            class _Cat:
                def __init__(self, parent):
                    self._parent = parent

                def listTables(self):
                    return []

                def listColumns(self, table):
                    return self._parent._columns_by_table.get(table, [])

            self.catalog = _Cat(self)

    cols = [
        SimpleNamespace(name="id", dataType="bigint", nullable=False),
        SimpleNamespace(name="value", dataType="double", nullable=True),
    ]
    runner = SparkConnectRunner(spark=_SparkWithColumns({"generation": cols}))

    result = runner.get_columns("generation")
    assert [c.name for c in result] == ["id", "value"]
    assert [c.dtype for c in result] == ["bigint", "double"]
    assert [c.nullable for c in result] == [False, True]


@pytest.mark.asyncio
async def test_introspect_schema_uses_runner_get_columns_for_spark(monkeypatch):
    """Schema introspection should call runner.get_columns instead of SQL probes."""
    from types import SimpleNamespace

    from datasight import schema as schema_mod

    sql_calls: list[str] = []

    async def _fake_run_sql(sql: str):
        sql_calls.append(sql)
        return pd.DataFrame()

    async def _no_count(*args, **kwargs):
        return None

    class _Cat:
        def listTables(self):
            return [SimpleNamespace(name="generation")]

        def listColumns(self, table):
            return [
                SimpleNamespace(name="id", dataType="bigint", nullable=False),
                SimpleNamespace(name="report_date", dataType="date", nullable=False),
            ]

    spark = _FakeSpark([])
    spark.catalog = _Cat()
    runner = SparkConnectRunner(spark=spark)
    monkeypatch.setattr(runner, "get_row_count", _no_count)

    tables = await schema_mod.introspect_schema(_fake_run_sql, runner=runner)

    assert [t.name for t in tables] == ["generation"]
    assert [c.name for c in tables[0].columns] == ["id", "report_date"]
    # Critically: the broken DuckDB/Postgres/SQLite SQL probes must not run.
    assert not any("DESCRIBE" in s for s in sql_calls), sql_calls
    assert not any("information_schema" in s for s in sql_calls), sql_calls
    assert not any("PRAGMA" in s for s in sql_calls), sql_calls


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
