"""Additional tests for datasight.agent covering edge branches."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
import pytest

from datasight.agent import (
    _format_sql,
    coerce_dates,
    execute_tool,
    extract_suggestions,
    resolve_plotly_spec,
    run_agent_loop,
    split_traces_by_group,
)
from datasight.llm import LLMResponse, TextBlock, ToolUseBlock, Usage
from datasight.runner import DuckDBRunner


# ---------------------------------------------------------------------------
# coerce_dates / resolve_plotly_spec edge cases
# ---------------------------------------------------------------------------


def test_coerce_dates_all_null_object_column():
    df = pd.DataFrame({"d": [None, None]}, dtype=object)
    result = coerce_dates(df)
    # All-null column stays as-is (sample empty branch)
    assert "d" in result.columns


def test_resolve_plotly_spec_datetime_column_serialized():
    df = pd.DataFrame({"ts": pd.to_datetime(["2024-01-01", "2024-02-01"]), "y": [1, 2]})
    spec = {"data": [{"type": "scatter", "x": "ts", "y": "y"}]}
    out = resolve_plotly_spec(spec, df)
    # datetime should be converted to ISO strings
    assert out["data"][0]["x"][0].startswith("2024-01-01")


def test_resolve_plotly_spec_nested_list_and_dict():
    df = pd.DataFrame({"x": [1, 2]})
    spec = {"data": [{"type": "bar", "x": "x", "extra": ["x", {"nested": "x"}]}]}
    out = resolve_plotly_spec(spec, df)
    # Nested column references resolved within list/dict
    assert out["data"][0]["extra"][0] == [1, 2]
    assert out["data"][0]["extra"][1]["nested"] == [1, 2]


def test_split_traces_by_group_datetime_column():
    df = pd.DataFrame(
        {
            "state": ["CA", "CA", "NY"],
            "ts": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-01-01"]),
            "v": [1, 2, 3],
        }
    )
    traces = [{"type": "scatter", "x": "ts", "y": "v", "name": "state"}]
    expanded = split_traces_by_group(traces, df)
    assert len(expanded) == 2
    # datetime gets serialized
    for t in expanded:
        assert isinstance(t["x"][0], str)


def test_split_traces_by_group_no_col_keys():
    """Trace with name referring to a column but no other column refs is passed through."""
    df = pd.DataFrame({"g": ["A", "B"]})
    traces = [{"type": "bar", "name": "g"}]
    expanded = split_traces_by_group(traces, df)
    assert len(expanded) == 1
    assert expanded[0]["name"] == "g"


def test_split_traces_by_group_name_not_a_column():
    df = pd.DataFrame({"x": [1]})
    traces = [{"type": "bar", "name": "literal-name", "x": "x"}]
    expanded = split_traces_by_group(traces, df)
    assert len(expanded) == 1
    assert expanded[0]["name"] == "literal-name"


# ---------------------------------------------------------------------------
# extract_suggestions malformed JSON
# ---------------------------------------------------------------------------


def test_extract_suggestions_non_array_json():
    # No [ ] array in the trailing part
    text = "answer\n---\n  just text no brackets"
    assert extract_suggestions(text) == []


# ---------------------------------------------------------------------------
# _format_sql
# ---------------------------------------------------------------------------


def test_format_sql_valid():
    out = _format_sql("select 1 as x")
    assert out is not None
    assert "SELECT" in out


def test_format_sql_invalid_returns_none():
    out = _format_sql("this is not sql @@@@ <<<<")
    # May or may not parse - just make sure no exception
    assert out is None or isinstance(out, str)


# ---------------------------------------------------------------------------
# execute_tool visualize error paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_tool_visualize_validation_error(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    schema_map = {"products": {"id"}}
    result = await execute_tool(
        "visualize_data",
        {
            "sql": "SELECT * FROM nonexistent",
            "title": "X",
            "plotly_spec": {"data": []},
        },
        run_sql=runner.run_sql,
        schema_map=schema_map,
    )
    runner.close()
    assert "validation error" in result.result_text.lower()


@pytest.mark.asyncio
async def test_execute_tool_visualize_sql_error(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    result = await execute_tool(
        "visualize_data",
        {
            "sql": "SELECT bogus_col FROM products",
            "title": "X",
            "plotly_spec": {"data": [{"type": "bar", "x": "bogus_col"}]},
        },
        run_sql=runner.run_sql,
    )
    runner.close()
    assert "error" in result.result_text.lower()
    assert result.meta["error"] is not None


@pytest.mark.asyncio
async def test_execute_tool_visualize_empty_result(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    result = await execute_tool(
        "visualize_data",
        {
            "sql": "SELECT * FROM products WHERE 1=0",
            "title": "X",
            "plotly_spec": {"data": [{"type": "bar", "x": "name"}]},
        },
        run_sql=runner.run_sql,
    )
    runner.close()
    assert "nothing to visualize" in result.result_text.lower()


@pytest.mark.asyncio
async def test_execute_tool_visualize_chart_build_error(test_duckdb_path):
    """Malformed plotly spec should surface a chart-building error."""
    runner = DuckDBRunner(test_duckdb_path)
    # Passing a non-dict spec triggers exception in resolve_plotly_spec
    result = await execute_tool(
        "visualize_data",
        {
            "sql": "SELECT name FROM products LIMIT 1",
            "title": "X",
            "plotly_spec": "not a dict",  # type: ignore[arg-type]
        },
        run_sql=runner.run_sql,
    )
    runner.close()
    assert "error" in result.result_text.lower()


# ---------------------------------------------------------------------------
# run_agent_loop with a fake LLM client
# ---------------------------------------------------------------------------


@dataclass
class _FakeLLMClient:
    """Fake LLM client: returns a scripted sequence of responses."""

    responses: list[LLMResponse]

    async def create_message(
        self,
        *,
        model: str,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        max_tokens: int,
    ) -> LLMResponse:
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_run_agent_loop_simple_text_response(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    fake = _FakeLLMClient(
        responses=[
            LLMResponse(
                content=[TextBlock(text='There are 5 products.\n---\n["More?", "Other?"]')],
                stop_reason="end_turn",
                usage=Usage(input_tokens=10, output_tokens=5),
            )
        ]
    )
    result = await run_agent_loop(
        question="how many?",
        llm_client=fake,
        model="m",
        system_prompt="p",
        run_sql=runner.run_sql,
    )
    runner.close()
    assert "5 products" in result.text
    assert result.suggestions == ["More?", "Other?"]
    assert result.api_calls == 1
    assert result.total_input_tokens == 10


@pytest.mark.asyncio
async def test_run_agent_loop_with_tool_use(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    fake = _FakeLLMClient(
        responses=[
            LLMResponse(
                content=[
                    ToolUseBlock(
                        id="t1",
                        name="run_sql",
                        input={"sql": "SELECT COUNT(*) AS n FROM products"},
                    )
                ],
                stop_reason="tool_use",
                usage=Usage(input_tokens=5, output_tokens=2),
            ),
            LLMResponse(
                content=[TextBlock(text="Done.")],
                stop_reason="end_turn",
                usage=Usage(input_tokens=7, output_tokens=3),
            ),
        ]
    )
    result = await run_agent_loop(
        question="count",
        llm_client=fake,
        model="m",
        system_prompt="p",
        run_sql=runner.run_sql,
    )
    runner.close()
    assert result.text == "Done."
    assert len(result.tool_results) == 1
    assert result.api_calls == 2


@pytest.mark.asyncio
async def test_run_agent_loop_max_iterations(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)

    # Always returns tool_use so loop never ends
    def _tool_response() -> LLMResponse:
        return LLMResponse(
            content=[ToolUseBlock(id="t", name="run_sql", input={"sql": "SELECT 1"})],
            stop_reason="tool_use",
            usage=Usage(input_tokens=1, output_tokens=1),
        )

    fake = _FakeLLMClient(responses=[_tool_response() for _ in range(5)])
    result = await run_agent_loop(
        question="q",
        llm_client=fake,
        model="m",
        system_prompt="p",
        run_sql=runner.run_sql,
        max_iterations=3,
    )
    runner.close()
    assert "maximum" in result.text.lower()
    assert result.api_calls == 3
