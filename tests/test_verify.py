"""Tests for datasight.verify."""

from __future__ import annotations

from typing import Any

import duckdb
import pandas as pd
import pytest

from datasight.llm import LLMResponse, TextBlock, ToolUseBlock, Usage
from datasight.verify import (
    AmbiguityResult,
    Check,
    Expectation,
    VerifyResult,
    analyze_ambiguity,
    check_result,
    parse_expectation,
    run_ambiguity_analysis,
    run_single_verification,
    run_verification,
)


# ---------------------------------------------------------------------------
# Fake LLM client
# ---------------------------------------------------------------------------


class FakeLLMClient:
    """Scripted LLM that returns pre-programmed responses in sequence."""

    def __init__(self, responses: list[LLMResponse]):
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def create_message(
        self,
        *,
        model: str,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        max_tokens: int,
    ) -> LLMResponse:
        self.calls.append(
            {
                "model": model,
                "system": system,
                "messages": [dict(m) for m in messages],
                "tools": tools,
                "max_tokens": max_tokens,
            }
        )
        if not self._responses:
            # Default: end the conversation
            return LLMResponse(content=[TextBlock(text="done")], stop_reason="end_turn")
        return self._responses.pop(0)


def _tool_use(sql: str, name: str = "run_sql", tool_id: str = "tu_1") -> LLMResponse:
    return LLMResponse(
        content=[ToolUseBlock(id=tool_id, name=name, input={"sql": sql})],
        stop_reason="tool_use",
        usage=Usage(),
    )


def _end(text: str = "done") -> LLMResponse:
    return LLMResponse(content=[TextBlock(text=text)], stop_reason="end_turn")


@pytest.fixture()
def run_sql(test_duckdb_path):
    """Async SQL runner bound to the test DuckDB."""
    conn = duckdb.connect(test_duckdb_path, read_only=True)

    async def _run(sql: str) -> pd.DataFrame:
        return conn.execute(sql).fetchdf()

    yield _run
    conn.close()


# ---------------------------------------------------------------------------
# parse_expectation
# ---------------------------------------------------------------------------


def test_parse_expectation_empty():
    assert parse_expectation({}) == Expectation()
    assert parse_expectation({"expected": {}}) == Expectation()


def test_parse_expectation_full():
    exp = parse_expectation(
        {
            "expected": {
                "row_count": 5,
                "min_row_count": 1,
                "max_row_count": 10,
                "columns": ["a", "b"],
                "contains": ["x"],
                "not_contains": ["y"],
            }
        }
    )
    assert exp.row_count == 5
    assert exp.min_row_count == 1
    assert exp.max_row_count == 10
    assert exp.columns == ["a", "b"]
    assert exp.contains == ["x"]
    assert exp.not_contains == ["y"]


# ---------------------------------------------------------------------------
# check_result
# ---------------------------------------------------------------------------


def test_check_result_row_count_pass_and_fail():
    df = pd.DataFrame({"x": [1, 2, 3]})
    checks = check_result(df, Expectation(row_count=3))
    assert len(checks) == 1 and checks[0].passed

    checks = check_result(df, Expectation(row_count=5))
    assert not checks[0].passed
    assert "expected 5" in checks[0].detail


def test_check_result_min_max_row_count():
    df = pd.DataFrame({"x": [1, 2, 3]})
    checks = check_result(df, Expectation(min_row_count=2, max_row_count=5))
    assert all(c.passed for c in checks)

    checks = check_result(df, Expectation(min_row_count=10, max_row_count=1))
    assert not any(c.passed for c in checks)


def test_check_result_columns():
    df = pd.DataFrame({"a": [1], "b": [2]})
    checks = check_result(df, Expectation(columns=["a", "b"]))
    assert checks[0].passed

    checks = check_result(df, Expectation(columns=["a", "c"]))
    assert not checks[0].passed


def test_check_result_contains_and_not_contains():
    df = pd.DataFrame({"name": ["Widget A", "Widget B"], "qty": [10, 5]})
    checks = check_result(
        df,
        Expectation(contains=["Widget A", "10"], not_contains=["Gadget X", "999"]),
    )
    # 2 contains + 2 not_contains = 4 checks, all should pass
    assert len(checks) == 4
    assert all(c.passed for c in checks)

    checks = check_result(df, Expectation(contains=["Missing"], not_contains=["Widget A"]))
    # contains check fails, not_contains also fails (Widget A is present)
    assert not any(c.passed for c in checks)


def test_check_result_contains_ignores_nan():
    df = pd.DataFrame({"x": ["a", None, "b"]})
    checks = check_result(df, Expectation(contains=["a", "b"]))
    assert all(c.passed for c in checks)


def test_verify_result_passed_property():
    r = VerifyResult(question="q", reference_sql="SELECT 1")
    assert r.passed  # no checks, no error -> passes vacuously

    r.checks = [Check(name="ok", passed=True)]
    assert r.passed

    r.checks.append(Check(name="bad", passed=False))
    assert not r.passed

    r2 = VerifyResult(question="q", reference_sql="", error="boom")
    assert not r2.passed


# ---------------------------------------------------------------------------
# run_single_verification
# ---------------------------------------------------------------------------


async def test_run_single_verification_passes_with_expectations(run_sql):
    client = FakeLLMClient(
        [
            _tool_use("SELECT COUNT(*) AS order_count FROM orders"),
            _end(),
        ]
    )
    result = await run_single_verification(
        question="How many orders?",
        reference_sql="SELECT COUNT(*) FROM orders",
        expectation=Expectation(row_count=1, columns=["order_count"]),
        llm_client=client,
        model="test-model",
        system_prompt="sys",
        run_sql=run_sql,
    )
    assert result.passed, result.checks
    assert result.generated_sql == "SELECT COUNT(*) AS order_count FROM orders"
    assert result.llm_iterations == 2
    assert result.error is None
    assert result.execution_time_ms >= 0


async def test_run_single_verification_fails_row_count(run_sql):
    client = FakeLLMClient([_tool_use("SELECT * FROM products"), _end()])
    result = await run_single_verification(
        question="all products",
        reference_sql="SELECT * FROM products",
        expectation=Expectation(row_count=99),
        llm_client=client,
        model="m",
        system_prompt="sys",
        run_sql=run_sql,
    )
    assert not result.passed
    assert any(c.name == "row_count" and not c.passed for c in result.checks)


async def test_run_single_verification_no_sql_generated(run_sql):
    client = FakeLLMClient([_end("I cannot help")])
    result = await run_single_verification(
        question="q",
        reference_sql="SELECT 1",
        expectation=Expectation(),
        llm_client=client,
        model="m",
        system_prompt="sys",
        run_sql=run_sql,
    )
    assert result.generated_sql is None
    assert result.error == "LLM did not generate any SQL query"
    assert not result.passed


async def test_run_single_verification_bad_final_sql(run_sql):
    # SQL parses but fails at execution time (nonexistent table).
    client = FakeLLMClient([_tool_use("SELECT * FROM nonexistent_table"), _end()])
    result = await run_single_verification(
        question="q",
        reference_sql="SELECT 1",
        expectation=Expectation(row_count=1),
        llm_client=client,
        model="m",
        system_prompt="sys",
        run_sql=run_sql,
    )
    # First tool-use errored inside the loop (captured as tool_result), and
    # the re-execution after the loop fails too, producing result.error.
    assert result.error is not None
    assert "Final SQL execution failed" in result.error


async def test_run_single_verification_no_expectations_reference_compare(run_sql):
    """With no expectations, verify compares generated vs reference SQL."""
    client = FakeLLMClient(
        [
            _tool_use("SELECT id, name FROM products ORDER BY id"),
            _end(),
        ]
    )
    result = await run_single_verification(
        question="products",
        reference_sql="SELECT id, name FROM products ORDER BY id",
        expectation=Expectation(),
        llm_client=client,
        model="m",
        system_prompt="sys",
        run_sql=run_sql,
    )
    assert result.passed
    names = {c.name for c in result.checks}
    assert "row_count_vs_reference" in names
    assert "columns_vs_reference" in names


async def test_run_single_verification_reference_sql_empty(run_sql):
    """Reference SQL returning 0 rows is flagged as broken."""
    client = FakeLLMClient([_tool_use("SELECT id FROM products"), _end()])
    result = await run_single_verification(
        question="q",
        reference_sql="SELECT id FROM products WHERE 1=0",
        expectation=Expectation(),
        llm_client=client,
        model="m",
        system_prompt="sys",
        run_sql=run_sql,
    )
    assert any(
        c.name == "reference_sql" and not c.passed and "0 rows" in c.detail for c in result.checks
    )


async def test_run_single_verification_reference_sql_errors(run_sql):
    client = FakeLLMClient([_tool_use("SELECT id FROM products"), _end()])
    result = await run_single_verification(
        question="q",
        reference_sql="SELECT * FROM not_a_table",
        expectation=Expectation(),
        llm_client=client,
        model="m",
        system_prompt="sys",
        run_sql=run_sql,
    )
    assert any(
        c.name == "reference_sql" and not c.passed and "reference SQL failed" in c.detail
        for c in result.checks
    )


async def test_run_single_verification_unknown_tool(run_sql):
    """An unknown tool call is answered with 'only run_sql is available'."""
    client = FakeLLMClient(
        [
            LLMResponse(
                content=[ToolUseBlock(id="t1", name="other_tool", input={})],
                stop_reason="tool_use",
            ),
            _tool_use("SELECT 1 AS x"),
            _end(),
        ]
    )
    result = await run_single_verification(
        question="q",
        reference_sql="SELECT 1",
        expectation=Expectation(row_count=1),
        llm_client=client,
        model="m",
        system_prompt="sys",
        run_sql=run_sql,
    )
    assert result.passed
    # The fake client was called 3 times total.
    assert len(client.calls) == 3


async def test_run_single_verification_tool_error_recovers(run_sql):
    """First SQL fails at execution; second is valid; verification still runs final SQL."""
    client = FakeLLMClient(
        [
            _tool_use("SELECT * FROM nope", tool_id="t1"),
            _tool_use("SELECT COUNT(*) AS c FROM orders", tool_id="t2"),
            _end(),
        ]
    )
    result = await run_single_verification(
        question="q",
        reference_sql="SELECT COUNT(*) FROM orders",
        expectation=Expectation(row_count=1),
        llm_client=client,
        model="m",
        system_prompt="sys",
        run_sql=run_sql,
    )
    assert result.generated_sql == "SELECT COUNT(*) AS c FROM orders"
    assert result.passed


async def test_run_single_verification_outer_exception(monkeypatch):
    """An exception raised by the LLM client itself populates result.error."""

    class Boom:
        async def create_message(self, **kwargs):
            raise RuntimeError("kaboom")

    async def _run(sql: str) -> pd.DataFrame:
        return pd.DataFrame()

    result = await run_single_verification(
        question="q",
        reference_sql="SELECT 1",
        expectation=Expectation(),
        llm_client=Boom(),
        model="m",
        system_prompt="sys",
        run_sql=_run,
    )
    assert result.error == "kaboom"
    assert not result.passed


# ---------------------------------------------------------------------------
# run_verification (loops over queries)
# ---------------------------------------------------------------------------


async def test_run_verification_multiple_queries(run_sql):
    client = FakeLLMClient(
        [
            _tool_use("SELECT COUNT(*) AS c FROM orders"),
            _end(),
            _tool_use("SELECT id FROM products"),
            _end(),
        ]
    )
    results = await run_verification(
        queries=[
            {
                "question": "count orders",
                "sql": "SELECT COUNT(*) FROM orders",
                "expected": {"row_count": 1},
            },
            {
                "question": "list products",
                "sql": "SELECT id FROM products",
                "expected": {"min_row_count": 1},
            },
        ],
        llm_client=client,
        model="m",
        system_prompt="sys",
        run_sql=run_sql,
    )
    assert len(results) == 2
    assert all(r.passed for r in results)


# ---------------------------------------------------------------------------
# Ambiguity analysis
# ---------------------------------------------------------------------------


async def test_analyze_ambiguity_parses_json():
    client = FakeLLMClient(
        [
            LLMResponse(
                content=[
                    TextBlock(
                        text='Here is my answer: {"is_ambiguous": true, '
                        '"ambiguities": ["rule 1"], '
                        '"suggested_revision": "Better question"}'
                    )
                ],
                stop_reason="end_turn",
            )
        ]
    )
    result = await analyze_ambiguity(
        question="trend over time",
        schema_context="schema",
        llm_client=client,
        model="m",
    )
    assert isinstance(result, AmbiguityResult)
    assert result.is_ambiguous is True
    assert result.ambiguities == ["rule 1"]
    assert result.suggested_revision == "Better question"


async def test_analyze_ambiguity_no_json():
    client = FakeLLMClient([LLMResponse(content=[TextBlock(text="nope")], stop_reason="end_turn")])
    result = await analyze_ambiguity(question="q", schema_context="", llm_client=client, model="m")
    assert result.is_ambiguous is False
    assert result.ambiguities == []


async def test_analyze_ambiguity_invalid_json():
    client = FakeLLMClient(
        [
            LLMResponse(
                content=[TextBlock(text="{not valid json}")],
                stop_reason="end_turn",
            )
        ]
    )
    result = await analyze_ambiguity(question="q", schema_context="", llm_client=client, model="m")
    assert result.is_ambiguous is False


async def test_run_ambiguity_analysis_loops_queries():
    client = FakeLLMClient(
        [
            LLMResponse(
                content=[
                    TextBlock(
                        text='{"is_ambiguous": false, "ambiguities": [], "suggested_revision": ""}'
                    )
                ],
                stop_reason="end_turn",
            ),
            LLMResponse(
                content=[
                    TextBlock(
                        text='{"is_ambiguous": true, "ambiguities": ["rule 2"], "suggested_revision": "Top 10 ..."}'
                    )
                ],
                stop_reason="end_turn",
            ),
        ]
    )
    results = await run_ambiguity_analysis(
        queries=[{"question": "clear"}, {"question": "top stuff"}],
        schema_context="schema",
        llm_client=client,
        model="m",
    )
    assert [r.is_ambiguous for r in results] == [False, True]
    assert results[1].suggested_revision == "Top 10 ..."
