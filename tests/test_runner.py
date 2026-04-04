"""Tests for SQL runner backends."""

import pytest

from datasight.runner import DuckDBRunner, SQLiteRunner


@pytest.mark.asyncio
async def test_duckdb_runner_basic_query(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    df = await runner.run_sql("SELECT COUNT(*) AS cnt FROM products")
    assert len(df) == 1
    assert df["cnt"].iloc[0] == 5
    runner.close()


@pytest.mark.asyncio
async def test_duckdb_runner_join(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    df = await runner.run_sql("""
        SELECT p.name, SUM(o.quantity) AS total_qty
        FROM orders o JOIN products p ON o.product_id = p.id
        GROUP BY p.name ORDER BY total_qty DESC
    """)
    assert len(df) == 5
    # Doohickey has 50 qty from one order — should be first
    assert df.iloc[0]["name"] == "Doohickey"
    assert df.iloc[0]["total_qty"] == 50
    runner.close()


@pytest.mark.asyncio
async def test_duckdb_runner_context_manager(test_duckdb_path):
    with DuckDBRunner(test_duckdb_path) as runner:
        df = await runner.run_sql("SELECT 1 AS x")
        assert df["x"].iloc[0] == 1


@pytest.mark.asyncio
async def test_duckdb_runner_error(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    with pytest.raises(Exception, match="nonexistent_table"):
        await runner.run_sql("SELECT * FROM nonexistent_table")
    runner.close()


@pytest.mark.asyncio
async def test_sqlite_runner_basic_query(test_sqlite_path):
    runner = SQLiteRunner(test_sqlite_path)
    df = await runner.run_sql("SELECT COUNT(*) AS cnt FROM products")
    assert len(df) == 1
    assert df["cnt"].iloc[0] == 5
    runner.close()


@pytest.mark.asyncio
async def test_sqlite_runner_join(test_sqlite_path):
    runner = SQLiteRunner(test_sqlite_path)
    df = await runner.run_sql("""
        SELECT p.name, SUM(o.quantity) AS total_qty
        FROM orders o JOIN products p ON o.product_id = p.id
        GROUP BY p.name ORDER BY total_qty DESC
    """)
    assert len(df) == 5
    assert df.iloc[0]["name"] == "Doohickey"
    assert df.iloc[0]["total_qty"] == 50
    runner.close()


@pytest.mark.asyncio
async def test_sqlite_runner_empty_result(test_sqlite_path):
    runner = SQLiteRunner(test_sqlite_path)
    df = await runner.run_sql("SELECT * FROM products WHERE id = 999")
    assert len(df) == 0
    assert "id" in df.columns
    runner.close()


@pytest.mark.asyncio
async def test_sqlite_runner_context_manager(test_sqlite_path):
    with SQLiteRunner(test_sqlite_path) as runner:
        df = await runner.run_sql("SELECT 1 AS x")
        assert df["x"].iloc[0] == 1
