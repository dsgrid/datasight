"""Tests for database schema introspection."""

import pytest

from datasight.runner import DuckDBRunner, SQLiteRunner
from datasight.schema import introspect_schema, format_schema_context


@pytest.mark.asyncio
async def test_introspect_duckdb(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    tables = await introspect_schema(runner.run_sql, runner=runner)
    runner.close()

    table_names = {t.name for t in tables}
    assert "products" in table_names
    assert "orders" in table_names

    products = next(t for t in tables if t.name == "products")
    col_names = {c.name for c in products.columns}
    assert {"id", "name", "category", "price"} <= col_names
    assert products.row_count == 5

    orders = next(t for t in tables if t.name == "orders")
    assert orders.row_count == 10


@pytest.mark.asyncio
async def test_introspect_sqlite(test_sqlite_path):
    runner = SQLiteRunner(test_sqlite_path)
    tables = await introspect_schema(runner.run_sql, runner=runner)
    runner.close()

    table_names = {t.name for t in tables}
    assert "products" in table_names
    assert "orders" in table_names

    products = next(t for t in tables if t.name == "products")
    col_names = {c.name for c in products.columns}
    assert {"id", "name", "category", "price"} <= col_names
    assert products.row_count == 5


def test_format_schema_context():
    from datasight.schema import TableInfo, ColumnInfo

    tables = [
        TableInfo(
            name="users",
            columns=[
                ColumnInfo(name="id", dtype="INTEGER", nullable=False),
                ColumnInfo(name="name", dtype="VARCHAR"),
            ],
            row_count=100,
        ),
    ]
    result = format_schema_context(tables, "# My DB\nSome description.")
    assert "users" in result
    assert "100" in result
    assert "id" in result
    assert "My DB" in result


def test_format_schema_context_no_description():
    from datasight.schema import TableInfo, ColumnInfo

    tables = [
        TableInfo(name="t", columns=[ColumnInfo(name="x", dtype="INT")], row_count=10),
    ]
    result = format_schema_context(tables, None)
    assert "t" in result
    assert "x" in result
