"""Additional tests for datasight.generate to cover edge branches."""

import pandas as pd
import pytest

from datasight.generate import (
    _clean_yaml_content,
    _extract_scalar,
    _extract_values,
    sample_enum_columns,
)
from datasight.schema import ColumnInfo, TableInfo


def test_extract_scalar_empty_df():
    assert _extract_scalar(pd.DataFrame(), "n") == 0


def test_extract_scalar_missing_column():
    df = pd.DataFrame({"other": [1]})
    assert _extract_scalar(df, "n") == 0


def test_extract_scalar_bad_value_returns_zero():
    df = pd.DataFrame({"n": ["not-a-number"]})
    assert _extract_scalar(df, "n") == 0


def test_extract_values_empty_df():
    assert _extract_values(pd.DataFrame(), "val") == []


def test_extract_values_missing_column():
    df = pd.DataFrame({"other": ["x"]})
    assert _extract_values(df, "val") == []


def test_clean_yaml_content_strips_fences_and_separators():
    content = "```yaml\n- question: a\n  sql: SELECT 1\n---\n- question: b\n  sql: SELECT 2\n```"
    cleaned = _clean_yaml_content(content)
    assert "```" not in cleaned
    assert "---" not in cleaned.split("\n")


@pytest.mark.asyncio
async def test_sample_enum_columns_handles_query_exception():
    """If a query throws, the column is skipped silently."""

    async def run_sql(sql):
        raise RuntimeError("boom")

    tables = [
        TableInfo(
            name="t",
            columns=[ColumnInfo(name="status", dtype="VARCHAR")],
            row_count=0,
        )
    ]
    result = await sample_enum_columns(run_sql, tables)
    assert result == ""


@pytest.mark.asyncio
async def test_sample_enum_columns_skips_zero_distinct():
    """Columns with 0 distinct values are skipped."""
    import duckdb

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE t (s VARCHAR)")  # empty table

    async def run_sql(sql):
        return conn.execute(sql).fetchdf()

    tables = [
        TableInfo(
            name="t",
            columns=[ColumnInfo(name="s", dtype="VARCHAR")],
            row_count=0,
        )
    ]
    result = await sample_enum_columns(run_sql, tables)
    assert result == ""
    conn.close()
