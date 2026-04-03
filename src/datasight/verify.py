"""
Query verification for datasight.

Runs each question from queries.yaml through the LLM pipeline, executes
the generated SQL, and compares results against expected values. Produces
a pass/fail report that can be used to validate correctness across
different LLM providers and models.
"""

from __future__ import annotations

import json as _json
import re as _re
import time
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from datasight.llm import LLMClient, TextBlock, ToolUseBlock, serialize_content
from datasight.prompts import VERIFY_TOOLS


@dataclass
class Expectation:
    """Expected properties of a query result."""

    row_count: int | None = None
    min_row_count: int | None = None
    max_row_count: int | None = None
    columns: list[str] | None = None
    contains: list[Any] | None = None
    not_contains: list[Any] | None = None


@dataclass
class Check:
    """A single pass/fail check within a verification result."""

    name: str
    passed: bool
    detail: str = ""


@dataclass
class VerifyResult:
    """Result of verifying a single query."""

    question: str
    reference_sql: str
    generated_sql: str | None = None
    checks: list[Check] = field(default_factory=list)
    error: str | None = None
    execution_time_ms: float = 0
    llm_iterations: int = 0

    @property
    def passed(self) -> bool:
        if self.error:
            return False
        return all(c.passed for c in self.checks)


def parse_expectation(entry: dict[str, Any]) -> Expectation:
    """Parse an expectation block from a queries.yaml entry."""
    raw = entry.get("expected", {})
    if not raw:
        return Expectation()
    return Expectation(
        row_count=raw.get("row_count"),
        min_row_count=raw.get("min_row_count"),
        max_row_count=raw.get("max_row_count"),
        columns=raw.get("columns"),
        contains=raw.get("contains"),
        not_contains=raw.get("not_contains"),
    )


def check_result(df: pd.DataFrame, expectation: Expectation) -> list[Check]:
    """Compare a DataFrame against expected properties."""
    checks: list[Check] = []

    if expectation.row_count is not None:
        passed = len(df) == expectation.row_count
        checks.append(
            Check(
                name="row_count",
                passed=passed,
                detail=f"expected {expectation.row_count}, got {len(df)}",
            )
        )

    if expectation.min_row_count is not None:
        passed = len(df) >= expectation.min_row_count
        checks.append(
            Check(
                name="min_row_count",
                passed=passed,
                detail=f"expected >= {expectation.min_row_count}, got {len(df)}",
            )
        )

    if expectation.max_row_count is not None:
        passed = len(df) <= expectation.max_row_count
        checks.append(
            Check(
                name="max_row_count",
                passed=passed,
                detail=f"expected <= {expectation.max_row_count}, got {len(df)}",
            )
        )

    if expectation.columns is not None:
        actual = list(df.columns)
        passed = actual == expectation.columns
        checks.append(
            Check(
                name="columns",
                passed=passed,
                detail=f"expected {expectation.columns}, got {actual}",
            )
        )

    if expectation.contains is not None or expectation.not_contains is not None:
        all_values = set()
        for col in df.columns:
            for val in df[col].dropna().astype(str):
                all_values.add(val)

        if expectation.contains is not None:
            for expected_val in expectation.contains:
                found = str(expected_val) in all_values
                checks.append(
                    Check(
                        name="contains",
                        passed=found,
                        detail=f"{'found' if found else 'missing'}: {expected_val!r}",
                    )
                )

        if expectation.not_contains is not None:
            for unexpected_val in expectation.not_contains:
                found = str(unexpected_val) in all_values
                checks.append(
                    Check(
                        name="not_contains",
                        passed=not found,
                        detail=f"{'unexpectedly found' if found else 'correctly absent'}: {unexpected_val!r}",
                    )
                )

    return checks


async def run_single_verification(
    *,
    question: str,
    reference_sql: str,
    expectation: Expectation,
    llm_client: LLMClient,
    model: str,
    system_prompt: str,
    run_sql,
    max_iterations: int = 10,
) -> VerifyResult:
    """Run a single question through the LLM and verify the result."""
    result = VerifyResult(question=question, reference_sql=reference_sql)
    t0 = time.perf_counter()

    messages: list[dict[str, Any]] = [{"role": "user", "content": question}]

    try:
        for iteration in range(max_iterations):
            response = await llm_client.create_message(
                model=model,
                max_tokens=4096,
                system=system_prompt,
                tools=VERIFY_TOOLS,
                messages=messages,
            )
            result.llm_iterations = iteration + 1

            if response.stop_reason == "tool_use":
                messages.append(
                    {"role": "assistant", "content": serialize_content(response.content)}
                )

                tool_results = []
                for block in response.content:
                    if not isinstance(block, ToolUseBlock):
                        continue
                    if block.name == "run_sql":
                        sql = block.input.get("sql", "")
                        # Capture the last SQL generated
                        result.generated_sql = sql
                        try:
                            df = await run_sql(sql)
                            csv = df.to_csv(index=False)
                            preview = csv if len(csv) <= 1000 else csv[:1000] + "\n(truncated)"
                            tool_text = (
                                f"{preview}\n\nReturned {len(df)} rows, {len(df.columns)} columns."
                            )
                            tool_results.append(
                                {
                                    "type": "tool_result",
                                    "tool_use_id": block.id,
                                    "content": tool_text,
                                }
                            )
                        except Exception as e:
                            tool_results.append(
                                {
                                    "type": "tool_result",
                                    "tool_use_id": block.id,
                                    "content": f"SQL error: {e}",
                                }
                            )
                    else:
                        tool_results.append(
                            {
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": "Only run_sql is available in verify mode.",
                            }
                        )
                messages.append({"role": "user", "content": tool_results})
                continue

            # end_turn — LLM finished
            break

        result.execution_time_ms = (time.perf_counter() - t0) * 1000

        # Now run the generated SQL to check expectations
        if result.generated_sql:
            try:
                df = await run_sql(result.generated_sql)
                result.checks = check_result(df, expectation)
            except Exception as e:
                result.error = f"Final SQL execution failed: {e}"
        else:
            result.error = "LLM did not generate any SQL query"

        # Also run the reference SQL and do a cross-check if we have expectations
        has_expectations = any(
            v is not None
            for v in [
                expectation.row_count,
                expectation.min_row_count,
                expectation.max_row_count,
                expectation.columns,
                expectation.contains,
                expectation.not_contains,
            ]
        )
        if not has_expectations and result.generated_sql:
            # No explicit expectations — compare against reference SQL output
            try:
                ref_df = await run_sql(reference_sql)
            except Exception as e:
                result.checks.append(
                    Check(
                        name="reference_sql",
                        passed=False,
                        detail=f"reference SQL failed: {e}",
                    )
                )
                ref_df = None

            if ref_df is not None:
                if ref_df.empty:
                    result.checks.append(
                        Check(
                            name="reference_sql",
                            passed=False,
                            detail="reference SQL returned 0 rows — likely a broken query",
                        )
                    )
                else:
                    try:
                        gen_df = await run_sql(result.generated_sql)
                        row_match = len(ref_df) == len(gen_df)
                        result.checks.append(
                            Check(
                                name="row_count_vs_reference",
                                passed=row_match,
                                detail=f"reference={len(ref_df)}, generated={len(gen_df)}",
                            )
                        )
                        col_match = list(ref_df.columns) == list(gen_df.columns)
                        result.checks.append(
                            Check(
                                name="columns_vs_reference",
                                passed=col_match,
                                detail=f"reference={list(ref_df.columns)}, generated={list(gen_df.columns)}",
                            )
                        )
                    except Exception as e:
                        result.checks.append(
                            Check(
                                name="generated_sql",
                                passed=False,
                                detail=f"generated SQL failed: {e}",
                            )
                        )

    except Exception as e:
        result.execution_time_ms = (time.perf_counter() - t0) * 1000
        result.error = str(e)

    return result


@dataclass
class AmbiguityResult:
    """Result of analyzing a single query for ambiguity."""

    question: str
    is_ambiguous: bool
    ambiguities: list[str] = field(default_factory=list)
    suggested_revision: str = ""


AMBIGUITY_PROMPT = """\
You are a SQL query reviewer. Given a natural language question and the database \
schema, check it against these mandatory rules. If ANY rule is triggered, the \
question is ambiguous.

Rules:
1. **Temporal granularity**: If the question involves trends, changes, or time \
(phrases like "over time", "trend", "growth", "by year/month", "historically") \
and does NOT explicitly state the granularity (daily, weekly, monthly, quarterly, \
yearly), it is ambiguous.
2. **Aggregation scope**: If the question says "top", "largest", "biggest", "most" \
without specifying a count, it is ambiguous.
3. **Metric choice**: If "largest", "biggest", "most" could refer to different \
numeric columns, it is ambiguous.
4. **Filter boundaries**: If the question uses relative terms like "recent", "old", \
"high", "low" without numeric thresholds or date ranges, it is ambiguous.
5. **Grouping level**: If the question references a category and multiple columns \
could serve as the grouping key, it is ambiguous.

Do NOT flag:
- Column alias names (total_mwh vs generation_mwh)
- Sort order when implied by context (top = descending)
- Formatting differences (date format, number precision)

Respond with ONLY a JSON object (no markdown, no backticks):
{
  "is_ambiguous": true/false,
  "ambiguities": ["rule number + description for each triggered rule"],
  "suggested_revision": "improved question text (or empty string if not ambiguous)"
}"""


async def analyze_ambiguity(
    *,
    question: str,
    schema_context: str,
    llm_client: LLMClient,
    model: str,
) -> AmbiguityResult:
    """Analyze a single question for ambiguity."""
    response = await llm_client.create_message(
        model=model,
        max_tokens=500,
        system=AMBIGUITY_PROMPT + "\n\n" + schema_context,
        tools=[],
        messages=[{"role": "user", "content": f"Question: {question}"}],
    )
    text = "".join(b.text for b in response.content if isinstance(b, TextBlock)).strip()

    # Parse JSON from response
    match = _re.search(r"\{.*\}", text, _re.DOTALL)
    if not match:
        return AmbiguityResult(question=question, is_ambiguous=False)
    try:
        data = _json.loads(match.group())
    except _json.JSONDecodeError:
        return AmbiguityResult(question=question, is_ambiguous=False)

    return AmbiguityResult(
        question=question,
        is_ambiguous=bool(data.get("is_ambiguous", False)),
        ambiguities=data.get("ambiguities", []),
        suggested_revision=data.get("suggested_revision", ""),
    )


async def run_ambiguity_analysis(
    *,
    queries: list[dict[str, Any]],
    schema_context: str,
    llm_client: LLMClient,
    model: str,
) -> list[AmbiguityResult]:
    """Analyze all queries for ambiguity."""
    results = []
    for entry in queries:
        result = await analyze_ambiguity(
            question=entry["question"],
            schema_context=schema_context,
            llm_client=llm_client,
            model=model,
        )
        results.append(result)
    return results


async def run_verification(
    *,
    queries: list[dict[str, Any]],
    llm_client: LLMClient,
    model: str,
    system_prompt: str,
    run_sql,
) -> list[VerifyResult]:
    """Run all queries through verification sequentially."""
    results = []
    for entry in queries:
        question = entry["question"]
        reference_sql = entry.get("sql", "")
        expectation = parse_expectation(entry)

        result = await run_single_verification(
            question=question,
            reference_sql=reference_sql,
            expectation=expectation,
            llm_client=llm_client,
            model=model,
            system_prompt=system_prompt,
            run_sql=run_sql,
        )
        results.append(result)

    return results
