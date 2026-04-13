"""Tests for the shared agent module."""

import pytest
import pandas as pd

from datasight.agent import (
    coerce_dates,
    df_to_html_table,
    execute_tool,
    extract_suggestions,
    resolve_plotly_spec,
    sql_error_hint,
    split_traces_by_group,
)
from datasight.runner import DuckDBRunner


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


def test_extract_suggestions_with_json():
    text = 'Here are results.\n\n---\n\n["What about X?", "Show Y"]'
    suggestions = extract_suggestions(text)
    assert suggestions == ["What about X?", "Show Y"]


def test_extract_suggestions_no_separator():
    assert extract_suggestions("Just some text.") == []


def test_extract_suggestions_malformed_json():
    text = "Result.\n\n---\n\nnot json"
    assert extract_suggestions(text) == []


def test_sql_error_hint_column_not_found():
    hint = sql_error_hint("Referenced column 'foo' not found in FROM clause")
    assert "HINT" in hint
    assert "JOIN" in hint


def test_sql_error_hint_ambiguous():
    hint = sql_error_hint("Column reference 'id' is ambiguous")
    assert "alias" in hint.lower()


def test_sql_error_hint_function_duckdb():
    hint = sql_error_hint("Scalar Function 'TO_DATE' does not exist", dialect="duckdb")
    assert "DATE_TRUNC" in hint


def test_sql_error_hint_function_postgres():
    hint = sql_error_hint("Scalar Function 'foo' does not exist", dialect="postgres")
    assert "PostgreSQL" in hint


def test_sql_error_hint_function_sqlite():
    hint = sql_error_hint("Scalar Function 'bar' does not exist", dialect="sqlite")
    assert "strftime" in hint


def test_sql_error_hint_no_match():
    hint = sql_error_hint("Some random error")
    assert hint == ""


def test_df_to_html_table_basic():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    html = df_to_html_table(df)
    assert "<table" in html
    assert "result-table" in html
    assert ">a<" in html
    assert ">1<" in html


def test_df_to_html_table_empty():
    df = pd.DataFrame()
    html = df_to_html_table(df)
    assert "no rows" in html.lower()


def test_df_to_html_table_escapes_html():
    df = pd.DataFrame({"col": ["<script>alert(1)</script>"]})
    html = df_to_html_table(df)
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_df_to_html_table_null_values():
    df = pd.DataFrame({"a": [1, None, 3]})
    html = df_to_html_table(df)
    assert "null-val" in html


def test_coerce_dates():
    df = pd.DataFrame({"d": ["2024-01-01", "2024-02-01"], "x": [1, 2]})
    result = coerce_dates(df)
    assert pd.api.types.is_datetime64_any_dtype(result["d"])
    assert result["x"].dtype == int


def test_coerce_dates_non_date_strings():
    df = pd.DataFrame({"s": ["hello", "world"]})
    result = coerce_dates(df)
    assert not pd.api.types.is_datetime64_any_dtype(result["s"])


# ---------------------------------------------------------------------------
# Plotly spec resolution
# ---------------------------------------------------------------------------


def test_resolve_plotly_spec_basic():
    df = pd.DataFrame({"cat": ["A", "B", "C"], "val": [10, 20, 30]})
    spec = {"data": [{"type": "bar", "x": "cat", "y": "val"}]}
    resolved = resolve_plotly_spec(spec, df)
    assert resolved["data"][0]["x"] == ["A", "B", "C"]
    assert resolved["data"][0]["y"] == [10, 20, 30]


def test_resolve_plotly_spec_with_layout():
    df = pd.DataFrame({"x": [1], "y": [2]})
    spec = {
        "data": [{"type": "bar", "x": "x", "y": "y"}],
        "layout": {"title": "Test"},
    }
    resolved = resolve_plotly_spec(spec, df)
    assert resolved["layout"]["title"] == "Test"


def test_resolve_plotly_spec_literal():
    """The literal wrapper is resolved inside non-name fields."""
    df = pd.DataFrame({"x": [1]})
    spec = {"data": [{"type": "bar", "x": "x", "marker": {"literal": "red"}}]}
    resolved = resolve_plotly_spec(spec, df)
    assert resolved["data"][0]["marker"] == "red"


def test_split_traces_by_group():
    df = pd.DataFrame(
        {
            "state": ["CA", "CA", "NY", "NY"],
            "year": [2023, 2024, 2023, 2024],
            "val": [10, 20, 30, 40],
        }
    )
    traces = [{"type": "scatter", "x": "year", "y": "val", "name": "state"}]
    expanded = split_traces_by_group(traces, df)
    assert len(expanded) == 2
    names = {t["name"] for t in expanded}
    assert names == {"CA", "NY"}


def test_resolve_plotly_spec_assigns_stable_trace_colors():
    df = pd.DataFrame(
        {
            "fuel": ["NG", "NG", "SUN", "SUN"],
            "year": [2023, 2024, 2023, 2024],
            "mwh": [10, 20, 5, 8],
        }
    )
    spec = {"data": [{"type": "bar", "x": "year", "y": "mwh", "name": "fuel"}]}

    resolved = resolve_plotly_spec(spec, df)

    colors = {trace["name"]: trace["marker"]["color"] for trace in resolved["data"]}
    assert colors == {"NG": "#f28e2b", "SUN": "#edc948"}


def test_resolve_plotly_spec_assigns_stable_bar_category_colors():
    df = pd.DataFrame({"fuel": ["NG", "SUN"], "mwh": [10, 5]})
    spec = {"data": [{"type": "bar", "x": "fuel", "y": "mwh"}]}

    resolved = resolve_plotly_spec(spec, df)

    assert resolved["data"][0]["marker"]["color"] == ["#f28e2b", "#edc948"]


# ---------------------------------------------------------------------------
# Tool execution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_tool_run_sql(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    result = await execute_tool(
        "run_sql",
        {"sql": "SELECT COUNT(*) AS cnt FROM products"},
        run_sql=runner.run_sql,
    )
    runner.close()
    assert result.df is not None
    assert result.df["cnt"].iloc[0] == 5
    assert result.result_html is not None
    assert result.meta["row_count"] == 1
    assert result.meta["error"] is None
    assert result.meta["validation"]["status"] == "not_run"


@pytest.mark.asyncio
async def test_execute_tool_run_sql_error(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    result = await execute_tool(
        "run_sql",
        {"sql": "SELECT * FROM nonexistent_table"},
        run_sql=runner.run_sql,
    )
    runner.close()
    assert "error" in result.result_text.lower()
    assert result.meta["error"] is not None


@pytest.mark.asyncio
async def test_execute_tool_validation_rejects_bad_table(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    schema_map = {"products": {"id", "name"}, "orders": {"id", "product_id"}}
    result = await execute_tool(
        "run_sql",
        {"sql": "SELECT * FROM hallucinated_table"},
        run_sql=runner.run_sql,
        schema_map=schema_map,
    )
    runner.close()
    assert "validation error" in result.result_text.lower()
    assert result.meta["validation"]["status"] == "failed"
    assert "hallucinated_table" in result.meta["validation"]["errors"][0]
    assert result.meta["error"] is not None


@pytest.mark.asyncio
async def test_execute_tool_validation_records_passed_status(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    schema_map = {"products": {"id", "name", "category", "price"}}
    result = await execute_tool(
        "run_sql",
        {"sql": "SELECT COUNT(*) AS cnt FROM products"},
        run_sql=runner.run_sql,
        schema_map=schema_map,
        turn_id="turn-1",
    )
    runner.close()
    assert result.meta["validation"] == {"status": "passed", "errors": []}
    assert result.meta["turn_id"] == "turn-1"
    assert result.meta["row_count"] == 1


@pytest.mark.asyncio
async def test_execute_tool_empty_result(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    result = await execute_tool(
        "run_sql",
        {"sql": "SELECT * FROM products WHERE id = 999"},
        run_sql=runner.run_sql,
    )
    runner.close()
    assert result.df is not None
    assert len(result.df) == 0
    assert "no rows" in result.result_text.lower()


@pytest.mark.asyncio
async def test_execute_tool_visualize(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    result = await execute_tool(
        "visualize_data",
        {
            "sql": "SELECT category, SUM(price) AS total FROM products GROUP BY category",
            "title": "Price by Category",
            "plotly_spec": {
                "data": [{"type": "bar", "x": "category", "y": "total"}],
            },
        },
        run_sql=runner.run_sql,
    )
    runner.close()
    assert result.plotly_spec is not None
    assert result.result_html is not None
    assert "Price by Category" in result.result_text


@pytest.mark.asyncio
async def test_execute_tool_unknown_tool(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    result = await execute_tool("bogus_tool", {}, run_sql=runner.run_sql)
    runner.close()
    assert "unknown tool" in result.result_text.lower()
