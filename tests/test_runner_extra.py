"""Additional tests for SQL runner backends to increase coverage."""

from __future__ import annotations

import sys
import types
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from datasight.exceptions import ConnectionError, QueryError, QueryTimeoutError
from datasight.runner import (
    DuckDBRunner,
    EphemeralDuckDBRunner,
    FlightSqlRunner,
    PostgresRunner,
    SQLiteRunner,
)


# ---------------------------------------------------------------------------
# DuckDBRunner
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duckdb_runner_closed_raises(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    runner.close()
    with pytest.raises(ConnectionError, match="closed"):
        await runner.run_sql("SELECT 1")


@pytest.mark.asyncio
async def test_duckdb_runner_double_close(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    runner.close()
    runner.close()  # Should not raise


def test_duckdb_runner_connect_failure(tmp_path):
    # Point at a directory to trigger connection failure
    bad_path = tmp_path / "nonexistent_dir" / "foo.duckdb"
    with pytest.raises(ConnectionError, match="Failed to connect to DuckDB"):
        DuckDBRunner(str(bad_path))


@pytest.mark.asyncio
async def test_duckdb_runner_aenter_aexit(test_duckdb_path):
    async with DuckDBRunner(test_duckdb_path) as runner:
        df = await runner.run_sql("SELECT 1 AS x")
        assert df["x"].iloc[0] == 1


@pytest.mark.asyncio
async def test_duckdb_runner_timeout(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path, query_timeout=0.001)

    # Patch _execute to simulate a slow query via asyncio.wait_for raising TimeoutError

    async def fake_wait_for(*args, **kwargs):
        raise TimeoutError()

    with patch("datasight.runner.asyncio.wait_for", side_effect=fake_wait_for):
        with pytest.raises(QueryTimeoutError, match="timed out"):
            await runner.run_sql("SELECT 1")
    runner.close()


# ---------------------------------------------------------------------------
# EphemeralDuckDBRunner
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ephemeral_duckdb_runner_basic():
    import duckdb

    conn = duckdb.connect(":memory:")
    runner = EphemeralDuckDBRunner(conn)
    df = await runner.run_sql("SELECT 42 AS answer")
    assert df["answer"].iloc[0] == 42
    runner.close()


@pytest.mark.asyncio
async def test_ephemeral_duckdb_runner_context_managers():
    import duckdb

    conn = duckdb.connect(":memory:")
    with EphemeralDuckDBRunner(conn) as r:
        df = await r.run_sql("SELECT 1 AS x")
        assert df["x"].iloc[0] == 1

    conn2 = duckdb.connect(":memory:")
    async with EphemeralDuckDBRunner(conn2) as r:
        df = await r.run_sql("SELECT 2 AS x")
        assert df["x"].iloc[0] == 2


@pytest.mark.asyncio
async def test_ephemeral_duckdb_runner_closed_raises():
    import duckdb

    conn = duckdb.connect(":memory:")
    runner = EphemeralDuckDBRunner(conn)
    runner.close()
    with pytest.raises(ConnectionError, match="closed"):
        await runner.run_sql("SELECT 1")


@pytest.mark.asyncio
async def test_ephemeral_duckdb_runner_query_error():
    import duckdb

    conn = duckdb.connect(":memory:")
    runner = EphemeralDuckDBRunner(conn)
    with pytest.raises(QueryError):
        await runner.run_sql("SELECT * FROM no_such_table")
    runner.close()


@pytest.mark.asyncio
async def test_ephemeral_duckdb_runner_timeout():
    import duckdb

    conn = duckdb.connect(":memory:")
    runner = EphemeralDuckDBRunner(conn, query_timeout=0.001)

    async def fake_wait_for(*args, **kwargs):
        raise TimeoutError()

    with patch("datasight.runner.asyncio.wait_for", side_effect=fake_wait_for):
        with pytest.raises(QueryTimeoutError):
            await runner.run_sql("SELECT 1")


# ---------------------------------------------------------------------------
# SQLiteRunner
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sqlite_runner_closed_raises(test_sqlite_path):
    runner = SQLiteRunner(test_sqlite_path)
    runner.close()
    with pytest.raises(ConnectionError, match="closed"):
        await runner.run_sql("SELECT 1")


@pytest.mark.asyncio
async def test_sqlite_runner_query_error(test_sqlite_path):
    runner = SQLiteRunner(test_sqlite_path)
    with pytest.raises(QueryError):
        await runner.run_sql("SELECT * FROM no_such_table")
    runner.close()


def test_sqlite_runner_connect_failure(tmp_path):
    bad_path = tmp_path / "nonexistent_dir" / "foo.sqlite"
    with pytest.raises(ConnectionError, match="Failed to connect to SQLite"):
        SQLiteRunner(str(bad_path))


@pytest.mark.asyncio
async def test_sqlite_runner_aenter_aexit(test_sqlite_path):
    async with SQLiteRunner(test_sqlite_path) as runner:
        df = await runner.run_sql("SELECT 1 AS x")
        assert df["x"].iloc[0] == 1


@pytest.mark.asyncio
async def test_sqlite_runner_timeout(test_sqlite_path):
    runner = SQLiteRunner(test_sqlite_path, query_timeout=0.001)

    async def fake_wait_for(*args, **kwargs):
        raise TimeoutError()

    with patch("datasight.runner.asyncio.wait_for", side_effect=fake_wait_for):
        with pytest.raises(QueryTimeoutError):
            await runner.run_sql("SELECT 1")
    runner.close()


# ---------------------------------------------------------------------------
# PostgresRunner — mock psycopg
# ---------------------------------------------------------------------------


def _make_fake_psycopg(connect_side_effect=None, execute_side_effect=None, rows=None):
    """Build a fake psycopg module."""
    fake_psycopg = cast(Any, types.ModuleType("psycopg"))

    class FakeError(Exception):
        pass

    fake_psycopg.Error = FakeError

    if connect_side_effect is not None:
        fake_psycopg.connect = MagicMock(side_effect=connect_side_effect)
        return fake_psycopg

    cursor = MagicMock()
    if execute_side_effect is not None:
        cursor.execute = MagicMock(side_effect=execute_side_effect)
    cursor.fetchall = MagicMock(return_value=rows if rows is not None else [])
    cursor.description = [("col1",), ("col2",)] if rows else None

    conn = MagicMock()
    conn.execute = MagicMock(return_value=cursor)
    if execute_side_effect is not None:
        conn.execute.side_effect = execute_side_effect
    conn.close = MagicMock()

    fake_psycopg.connect = MagicMock(return_value=conn)
    fake_psycopg._fake_conn = conn
    fake_psycopg._fake_cursor = cursor
    return fake_psycopg


@pytest.mark.asyncio
async def test_postgres_runner_connect_with_params():
    fake = _make_fake_psycopg(rows=[(1, "a"), (2, "b")])
    with patch.dict(sys.modules, {"psycopg": fake}):
        runner = PostgresRunner(
            host="db.example.com",
            port=5432,
            dbname="mydb",
            user="u",
            password="p",
        )
        assert runner._connection_info == "db.example.com:5432/mydb"
        df = await runner.run_sql("SELECT * FROM foo")
        assert list(df.columns) == ["col1", "col2"]
        assert len(df) == 2
        runner.close()
        # Double close is safe
        runner.close()


@pytest.mark.asyncio
async def test_postgres_runner_connect_with_url():
    fake = _make_fake_psycopg(rows=[])
    with patch.dict(sys.modules, {"psycopg": fake}):
        runner = PostgresRunner(url="postgresql://u:p@h/db")
        assert runner._connection_info == "via URL"
        df = await runner.run_sql("SELECT * FROM empty")
        assert len(df) == 0
        runner.close()


@pytest.mark.asyncio
async def test_postgres_runner_context_managers():
    fake = _make_fake_psycopg(rows=[(1, "a")])
    with patch.dict(sys.modules, {"psycopg": fake}):
        with PostgresRunner(host="h", dbname="d") as r:
            df = await r.run_sql("SELECT 1")
            assert len(df) == 1

    fake2 = _make_fake_psycopg(rows=[(1, "a")])
    with patch.dict(sys.modules, {"psycopg": fake2}):
        async with PostgresRunner(host="h", dbname="d") as r:
            df = await r.run_sql("SELECT 1")
            assert len(df) == 1


def test_postgres_runner_connect_failure():
    fake = cast(Any, types.ModuleType("psycopg"))

    class FakeError(Exception):
        pass

    fake.Error = FakeError
    fake.connect = MagicMock(side_effect=FakeError("connection refused"))

    with patch.dict(sys.modules, {"psycopg": fake}):
        with pytest.raises(ConnectionError, match="Failed to connect to PostgreSQL"):
            PostgresRunner(host="h", dbname="d")


@pytest.mark.asyncio
async def test_postgres_runner_query_error():
    fake = _make_fake_psycopg(rows=[(1,)])
    with patch.dict(sys.modules, {"psycopg": fake}):
        runner = PostgresRunner(host="h", dbname="d")
        # Make execute raise the fake Error
        runner._conn.execute.side_effect = fake.Error("syntax error")  # ty: ignore[unresolved-attribute, invalid-assignment]
        with pytest.raises(QueryError, match="syntax error"):
            await runner.run_sql("BADSQL")
        runner.close()


@pytest.mark.asyncio
async def test_postgres_runner_closed_raises():
    fake = _make_fake_psycopg(rows=[])
    with patch.dict(sys.modules, {"psycopg": fake}):
        runner = PostgresRunner(host="h", dbname="d")
        runner.close()
        with pytest.raises(ConnectionError, match="closed"):
            await runner.run_sql("SELECT 1")


@pytest.mark.asyncio
async def test_postgres_runner_timeout():
    fake = _make_fake_psycopg(rows=[])
    with patch.dict(sys.modules, {"psycopg": fake}):
        runner = PostgresRunner(host="h", dbname="d", query_timeout=0.001)

        async def fake_wait_for(*args, **kwargs):
            raise TimeoutError()

        with patch("datasight.runner.asyncio.wait_for", side_effect=fake_wait_for):
            with pytest.raises(QueryTimeoutError):
                await runner.run_sql("SELECT 1")
        runner.close()


def test_postgres_runner_close_swallows_errors():
    fake = _make_fake_psycopg(rows=[])
    with patch.dict(sys.modules, {"psycopg": fake}):
        runner = PostgresRunner(host="h", dbname="d")
        runner._conn.close.side_effect = RuntimeError("boom")  # ty: ignore[unresolved-attribute, invalid-assignment]
        runner.close()  # Should not raise


# ---------------------------------------------------------------------------
# FlightSqlRunner — mock adbc_driver_flightsql.dbapi
# ---------------------------------------------------------------------------


def _make_fake_flightsql(connect_side_effect=None):
    """Build a fake adbc_driver_flightsql.dbapi module."""
    fake_pkg = cast(Any, types.ModuleType("adbc_driver_flightsql"))
    fake_dbapi = cast(Any, types.ModuleType("adbc_driver_flightsql.dbapi"))

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor

    # fetch_arrow_table returns something with to_pandas()
    fake_table = MagicMock()
    fake_table.to_pandas.return_value = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    cursor.fetch_arrow_table.return_value = fake_table

    if connect_side_effect is not None:
        fake_dbapi.connect = MagicMock(side_effect=connect_side_effect)
    else:
        fake_dbapi.connect = MagicMock(return_value=conn)

    fake_dbapi._fake_conn = conn
    fake_dbapi._fake_cursor = cursor
    fake_pkg.dbapi = fake_dbapi
    return fake_pkg, fake_dbapi


@pytest.mark.asyncio
async def test_flightsql_runner_basic():
    fake_pkg, fake_dbapi = _make_fake_flightsql()
    with patch.dict(
        sys.modules,
        {"adbc_driver_flightsql": fake_pkg, "adbc_driver_flightsql.dbapi": fake_dbapi},
    ):
        runner = FlightSqlRunner(
            uri="grpc://host:1234",
            username="u",
            password="p",
            token="tok",
        )
        df = await runner.run_sql("SELECT 1; ")
        assert len(df) == 2
        assert list(df.columns) == ["a", "b"]
        # Check connect received db_kwargs with all creds
        call_kwargs = fake_dbapi.connect.call_args.kwargs
        assert call_kwargs["uri"] == "grpc://host:1234"
        assert call_kwargs["db_kwargs"]["username"] == "u"
        assert call_kwargs["db_kwargs"]["password"] == "p"
        assert call_kwargs["db_kwargs"]["authorization"] == "Bearer tok"
        runner.close()
        runner.close()  # Double close ok


@pytest.mark.asyncio
async def test_flightsql_runner_context_managers():
    fake_pkg, fake_dbapi = _make_fake_flightsql()
    with patch.dict(
        sys.modules,
        {"adbc_driver_flightsql": fake_pkg, "adbc_driver_flightsql.dbapi": fake_dbapi},
    ):
        with FlightSqlRunner() as r:
            df = await r.run_sql("SELECT 1")
            assert len(df) == 2

    fake_pkg2, fake_dbapi2 = _make_fake_flightsql()
    with patch.dict(
        sys.modules,
        {"adbc_driver_flightsql": fake_pkg2, "adbc_driver_flightsql.dbapi": fake_dbapi2},
    ):
        async with FlightSqlRunner() as r:
            df = await r.run_sql("SELECT 1")
            assert len(df) == 2


def test_flightsql_runner_connect_failure():
    fake_pkg, fake_dbapi = _make_fake_flightsql(connect_side_effect=RuntimeError("bad"))
    with patch.dict(
        sys.modules,
        {"adbc_driver_flightsql": fake_pkg, "adbc_driver_flightsql.dbapi": fake_dbapi},
    ):
        with pytest.raises(ConnectionError, match="Failed to connect to Flight SQL"):
            FlightSqlRunner()


@pytest.mark.asyncio
async def test_flightsql_runner_query_error():
    fake_pkg, fake_dbapi = _make_fake_flightsql()
    with patch.dict(
        sys.modules,
        {"adbc_driver_flightsql": fake_pkg, "adbc_driver_flightsql.dbapi": fake_dbapi},
    ):
        runner = FlightSqlRunner()
        fake_dbapi._fake_cursor.execute.side_effect = RuntimeError("sql error")
        with pytest.raises(QueryError, match="sql error"):
            await runner.run_sql("BAD SQL")
        runner.close()


@pytest.mark.asyncio
async def test_flightsql_runner_closed_execute_raises():
    fake_pkg, fake_dbapi = _make_fake_flightsql()
    with patch.dict(
        sys.modules,
        {"adbc_driver_flightsql": fake_pkg, "adbc_driver_flightsql.dbapi": fake_dbapi},
    ):
        runner = FlightSqlRunner()
        runner.close()
        with pytest.raises(ConnectionError, match="closed"):
            await runner.run_sql("SELECT 1")


@pytest.mark.asyncio
async def test_flightsql_runner_timeout():
    fake_pkg, fake_dbapi = _make_fake_flightsql()
    with patch.dict(
        sys.modules,
        {"adbc_driver_flightsql": fake_pkg, "adbc_driver_flightsql.dbapi": fake_dbapi},
    ):
        runner = FlightSqlRunner(timeout=0.001)

        async def fake_wait_for(*args, **kwargs):
            raise TimeoutError()

        with patch("datasight.runner.asyncio.wait_for", side_effect=fake_wait_for):
            with pytest.raises(QueryTimeoutError):
                await runner.run_sql("SELECT 1")
        runner.close()


def test_flightsql_runner_get_table_names_closed():
    fake_pkg, fake_dbapi = _make_fake_flightsql()
    with patch.dict(
        sys.modules,
        {"adbc_driver_flightsql": fake_pkg, "adbc_driver_flightsql.dbapi": fake_dbapi},
    ):
        runner = FlightSqlRunner()
        runner.close()
        with pytest.raises(ConnectionError, match="closed"):
            runner.get_table_names()


def test_flightsql_runner_get_table_names_error():
    fake_pkg, fake_dbapi = _make_fake_flightsql()
    with patch.dict(
        sys.modules,
        {"adbc_driver_flightsql": fake_pkg, "adbc_driver_flightsql.dbapi": fake_dbapi},
    ):
        runner = FlightSqlRunner()
        runner._conn.adbc_get_objects.side_effect = RuntimeError("no objects")  # ty: ignore[unresolved-attribute, invalid-assignment]
        with pytest.raises(QueryError, match="Failed to get table names"):
            runner.get_table_names()
        runner.close()


def test_flightsql_runner_get_table_names_success():
    fake_pkg, fake_dbapi = _make_fake_flightsql()
    with patch.dict(
        sys.modules,
        {"adbc_driver_flightsql": fake_pkg, "adbc_driver_flightsql.dbapi": fake_dbapi},
    ):
        runner = FlightSqlRunner()

        # Build a fake pyarrow-like objects structure.
        # objects.column("catalog_db_schemas") yields a sequence of catalog_batch;
        # each catalog_batch iterates to schema_entry dicts with "db_schema_tables";
        # each tables entry iterates to dicts with "table_name" that has .as_py().
        class _Name:
            def __init__(self, n):
                self.n = n

            def as_py(self):
                return self.n

        table_entries = [{"table_name": _Name("t1")}, {"table_name": _Name("t2")}]
        schema_entries = [{"db_schema_tables": table_entries}]
        catalog_batches = [schema_entries, None]  # include a None to cover branch

        column_result = catalog_batches

        objects = MagicMock()
        objects.column.return_value = column_result
        read_all_result = objects
        runner._conn.adbc_get_objects.return_value.read_all.return_value = read_all_result  # ty: ignore[unresolved-attribute]

        names = runner.get_table_names()
        assert names == ["t1", "t2"]
        runner.close()


def test_flightsql_runner_close_swallows_errors():
    fake_pkg, fake_dbapi = _make_fake_flightsql()
    with patch.dict(
        sys.modules,
        {"adbc_driver_flightsql": fake_pkg, "adbc_driver_flightsql.dbapi": fake_dbapi},
    ):
        runner = FlightSqlRunner()
        runner._conn.close.side_effect = RuntimeError("boom")  # ty: ignore[unresolved-attribute, invalid-assignment]
        runner.close()  # Should not raise
