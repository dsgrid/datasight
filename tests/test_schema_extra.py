"""Extra schema introspection tests covering fallbacks and error paths."""

from __future__ import annotations

import pandas as pd
import pytest

from datasight.schema import (
    _get_row_count,
    _quote_identifier,
    _validate_identifier,
    introspect_schema,
)


# ---------------------------------------------------------------------------
# _validate_identifier
# ---------------------------------------------------------------------------


def test_validate_identifier_rejects_unsafe():
    with pytest.raises(ValueError, match="Unsafe identifier"):
        _validate_identifier("drop; users")


def test_validate_identifier_allows_safe_characters():
    assert _validate_identifier("my_table.schema-1") == "my_table.schema-1"


def test_quote_identifier_wraps_plain_name():
    assert _quote_identifier("users") == '"users"'


def test_quote_identifier_allows_spaces():
    assert _quote_identifier("Host Name") == '"Host Name"'


def test_quote_identifier_escapes_embedded_quote():
    assert _quote_identifier('weird"name') == '"weird""name"'


def test_quote_identifier_rejects_newline():
    with pytest.raises(ValueError, match="Unsafe identifier"):
        _quote_identifier("bad\nname")


# ---------------------------------------------------------------------------
# introspect_schema: ADBC runner path + fallback + no-tables warning
# ---------------------------------------------------------------------------


class _ADBCRunner:
    def __init__(self, tables: list[str] | None, raises: bool = False):
        self._tables = tables
        self._raises = raises

    def get_table_names(self) -> list[str]:
        if self._raises:
            raise RuntimeError("ADBC boom")
        return list(self._tables or [])


@pytest.mark.asyncio
async def test_introspect_schema_uses_adbc_runner():
    async def run_sql(sql: str) -> pd.DataFrame:
        if "DESCRIBE" in sql:
            return pd.DataFrame([{"column_name": "c1", "column_type": "INTEGER", "null": "NO"}])
        if "COUNT(*)" in sql:
            return pd.DataFrame([{"cnt": 7}])
        return pd.DataFrame()

    runner = _ADBCRunner(tables=["t1"])
    tables = await introspect_schema(run_sql, runner=runner)
    assert len(tables) == 1
    assert tables[0].name == "t1"
    assert tables[0].row_count == 7
    assert tables[0].columns[0].nullable is False


@pytest.mark.asyncio
async def test_introspect_schema_falls_back_when_adbc_raises():
    async def run_sql(sql: str) -> pd.DataFrame:
        if "SHOW TABLES" in sql:
            return pd.DataFrame({"name": ["alpha"]})
        if "DESCRIBE" in sql:
            return pd.DataFrame([{"column_name": "id", "column_type": "INT", "null": "YES"}])
        if "COUNT(*)" in sql:
            return pd.DataFrame([{"cnt": 1}])
        return pd.DataFrame()

    runner = _ADBCRunner(tables=None, raises=True)
    tables = await introspect_schema(run_sql, runner=runner)
    assert [t.name for t in tables] == ["alpha"]


@pytest.mark.asyncio
async def test_introspect_schema_no_tables_warning():
    async def run_sql(sql: str) -> pd.DataFrame:
        return pd.DataFrame()

    tables = await introspect_schema(run_sql, runner=None)
    assert tables == []


# ---------------------------------------------------------------------------
# _get_table_names fallbacks exercised via introspect_schema
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_introspect_schema_information_schema_fallback():
    calls: list[str] = []

    async def run_sql(sql: str) -> pd.DataFrame:
        calls.append(sql)
        # SHOW TABLES returns empty
        if "SHOW TABLES" in sql:
            return pd.DataFrame()
        # information_schema.tables returns data
        if "information_schema.tables" in sql:
            return pd.DataFrame({"table_name": ["beta"]})
        # information_schema.columns empty -> fall through
        if "information_schema.columns" in sql:
            return pd.DataFrame()
        # DESCRIBE empty -> fall through
        if "DESCRIBE" in sql:
            return pd.DataFrame()
        # PRAGMA returns columns
        if "PRAGMA table_info" in sql:
            return pd.DataFrame([{"name": "col_a", "type": "TEXT", "notnull": 1}])
        if "COUNT(*)" in sql:
            return pd.DataFrame([{"cnt": 3}])
        return pd.DataFrame()

    tables = await introspect_schema(run_sql, runner=None)
    assert len(tables) == 1
    assert tables[0].name == "beta"
    assert tables[0].columns[0].name == "col_a"
    assert tables[0].columns[0].nullable is False


@pytest.mark.asyncio
async def test_introspect_schema_sqlite_master_fallback():
    async def run_sql(sql: str) -> pd.DataFrame:
        if "SHOW TABLES" in sql:
            return pd.DataFrame()
        if "information_schema.tables" in sql:
            return pd.DataFrame()
        if "sqlite_master" in sql:
            return pd.DataFrame({"name": ["gamma"]})
        if "DESCRIBE" in sql:
            return pd.DataFrame()
        if "information_schema.columns" in sql:
            return pd.DataFrame()
        if "PRAGMA table_info" in sql:
            return pd.DataFrame()
        # SELECT * FROM "gamma" LIMIT 0 -> returns empty df with columns
        if "LIMIT 0" in sql:
            return pd.DataFrame(columns=["only_col"])
        if "COUNT(*)" in sql:
            # Trigger _get_row_count coercion failure path
            return pd.DataFrame([{"cnt": "not a number"}])
        return pd.DataFrame()

    tables = await introspect_schema(run_sql, runner=None)
    assert len(tables) == 1
    assert tables[0].name == "gamma"
    assert [c.name for c in tables[0].columns] == ["only_col"]
    assert tables[0].row_count is None  # coercion failed


# ---------------------------------------------------------------------------
# _get_row_count error paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_row_count_returns_none_on_empty():
    async def run_sql(sql: str) -> pd.DataFrame:
        return pd.DataFrame()

    assert await _get_row_count(run_sql, "t") is None


@pytest.mark.asyncio
async def test_get_row_count_returns_none_on_type_error():
    async def run_sql(sql: str) -> pd.DataFrame:
        return pd.DataFrame([{"cnt": object()}])

    assert await _get_row_count(run_sql, "t") is None


@pytest.mark.asyncio
async def test_get_row_count_handles_run_sql_exception():
    async def run_sql(sql: str) -> pd.DataFrame:
        raise RuntimeError("boom")

    # _run wraps the exception and returns empty DataFrame -> None
    assert await _get_row_count(run_sql, "t") is None
