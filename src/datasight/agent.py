"""
Shared agent loop for datasight.

Provides a reusable LLM agent loop that handles tool execution, SQL
validation, and multi-turn conversations. Used by both the web UI
(streaming via SSE) and the headless CLI.
"""

from __future__ import annotations

import json
import re
import time
import traceback
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from loguru import logger

from datasight.chart import build_chart_html
from datasight.exceptions import QueryError
from datasight.llm import LLMClient, TextBlock, ToolUseBlock, serialize_content
from datasight.prompts import WEB_TOOLS
from datasight.runner import RunSql
from datasight.sql_validation import validate_sql


# ---------------------------------------------------------------------------
# Data helpers (shared with web app)
# ---------------------------------------------------------------------------


def coerce_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Try to parse object columns that look like dates into datetime."""
    df = df.copy()
    for col in df.select_dtypes(include=["object"]).columns:
        sample = df[col].dropna().head(5)
        if sample.empty:
            continue
        try:
            parsed = pd.to_datetime(sample, format="mixed")
            if parsed.notna().all():
                df[col] = pd.to_datetime(df[col], format="mixed")
        except (ValueError, TypeError):
            continue
    return df


def split_traces_by_group(traces: list[dict[str, Any]], df: pd.DataFrame) -> list[dict[str, Any]]:
    """Auto-split traces when ``name`` references a DataFrame column."""
    columns = set(df.columns)
    expanded: list[dict[str, Any]] = []

    for trace in traces:
        group_col = trace.get("name")
        if not isinstance(group_col, str) or group_col not in columns:
            expanded.append(trace)
            continue

        col_keys = [
            k
            for k, v in trace.items()
            if k not in ("type", "mode", "name") and isinstance(v, str) and v in columns
        ]
        if not col_keys:
            expanded.append(trace)
            continue

        for group_value, sub_df in df.groupby(group_col, sort=True):
            new_trace = {k: v for k, v in trace.items() if k != "name"}
            new_trace["name"] = str(group_value)
            for k in col_keys:
                col_name = trace[k]
                series = sub_df[col_name]
                if pd.api.types.is_datetime64_any_dtype(series):
                    new_trace[k] = series.dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
                else:
                    new_trace[k] = series.tolist()
            expanded.append(new_trace)

    return expanded


def resolve_plotly_spec(spec: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
    """Replace column name references in a Plotly spec with actual data arrays."""
    df = coerce_dates(df)
    columns = set(df.columns)

    raw_traces = spec.get("data", [])
    raw_traces = split_traces_by_group(raw_traces, df)

    def _resolve_value(val: Any) -> Any:
        if isinstance(val, dict):
            if "literal" in val and len(val) == 1:
                return val["literal"]
            return {k: _resolve_value(v) for k, v in val.items()}
        if isinstance(val, list):
            return [_resolve_value(item) for item in val]
        if isinstance(val, str) and val in columns:
            series = df[val]
            if pd.api.types.is_datetime64_any_dtype(series):
                return series.dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
            return series.tolist()
        return val

    resolved: dict[str, Any] = {"data": []}
    for trace in raw_traces:
        resolved_trace = {}
        for key, val in trace.items():
            if key in ("type", "name"):
                resolved_trace[key] = val
            else:
                resolved_trace[key] = _resolve_value(val)
        resolved["data"].append(resolved_trace)
    if "layout" in spec:
        resolved["layout"] = spec["layout"]
    return resolved


def sql_error_hint(error_msg: str, dialect: str = "duckdb") -> str:
    """Return a short hint to help the model fix common SQL errors."""
    hints: list[str] = []
    lower = error_msg.lower()
    if "not found in from clause" in lower or "referenced column" in lower:
        hints.append(
            "HINT: Column not found in FROM tables. Check the schema and add a JOIN if needed."
        )
    if "ambiguous" in lower:
        hints.append("HINT: Qualify the column with a table alias.")
    if "scalar function" in lower and "does not exist" in lower:
        match dialect:
            case "postgres":
                hints.append(
                    "HINT: PostgreSQL syntax — use DATE_TRUNC, EXTRACT, TO_CHAR, col::type."
                )
            case "sqlite":
                hints.append(
                    "HINT: SQLite syntax — use strftime(), date(), datetime(). "
                    "No DATE_TRUNC or EXTRACT."
                )
            case _:
                hints.append(
                    "HINT: DuckDB syntax — use DATE_TRUNC, EXTRACT, STRFTIME, col::DATE. "
                    "Not TO_DATE, DATE_FORMAT, TO_CHAR."
                )
    if "table with name" in lower and "does not exist" in lower:
        hints.append("HINT: Table not found. Check the schema for available tables.")
    if "syntax error" in lower:
        hints.append("HINT: Write a plain SQL SELECT query. No embedded data or XML tags.")
    return ("\n" + "\n".join(hints)) if hints else ""


def extract_suggestions(text: str) -> list[str]:
    """Extract follow-up suggestions from ``---`` separator + JSON array."""
    parts = re.split(r"\n---\s*\n", text, maxsplit=1)
    if len(parts) < 2:
        return []
    match = re.search(r"\[.*\]", parts[1], re.DOTALL)
    if not match:
        return []
    try:
        return json.loads(match.group())
    except (json.JSONDecodeError, TypeError):
        return []


def _escape_html(text: str) -> str:
    """Escape HTML special characters."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def df_to_html_table(df: pd.DataFrame, max_rows: int = 200) -> str:
    """Render a DataFrame as a styled HTML table string."""
    if df.empty:
        return "<p class='empty-result'>Query returned no rows.</p>"

    display_df = df.head(max_rows)

    html = f"<div class='result-table-wrap' data-total-rows='{len(df)}'>"
    html += (
        "<div class='table-toolbar'>"
        "<input class='table-filter' placeholder='Filter rows...'>"
        "<button class='export-csv-btn'>Download CSV</button>"
        "</div>"
    )
    html += "<table class='result-table'><thead><tr>"
    for i, col in enumerate(display_df.columns):
        html += f"<th data-col='{i}'>{_escape_html(str(col))}<span class='sort-arrow'></span></th>"
    html += "</tr></thead><tbody>"
    for _, row in display_df.iterrows():
        html += "<tr>"
        for col in display_df.columns:
            val = row[col]
            if pd.isna(val):
                html += "<td class='null-val'>NULL</td>"
            else:
                html += f"<td>{_escape_html(str(val))}</td>"
        html += "</tr>"
    html += "</tbody></table>"
    html += "<div class='table-pagination'></div>"
    html += "</div>"
    return html


# ---------------------------------------------------------------------------
# SQL execution with validation
# ---------------------------------------------------------------------------


@dataclass
class SqlExecutionResult:
    """Result of executing SQL with validation."""

    df: pd.DataFrame | None = None
    elapsed_ms: float = 0.0
    error: str | None = None
    validation_error: str | None = None


async def execute_sql_with_validation(
    sql: str,
    run_sql: RunSql,
    schema_map: dict[str, set[str]] | None = None,
    dialect: str = "duckdb",
) -> SqlExecutionResult:
    """Execute SQL with optional schema validation.

    Parameters
    ----------
    sql:
        The SQL query to execute.
    run_sql:
        Async function that executes SQL and returns a DataFrame.
    schema_map:
        Optional mapping of table names to column names for validation.
    dialect:
        SQL dialect for validation and error hints.

    Returns
    -------
    SqlExecutionResult with the DataFrame or error information.
    """
    # Validate SQL against schema if available
    if schema_map:
        vr = validate_sql(sql, schema_map, dialect=dialect)
        if not vr.valid:
            logger.warning(f"SQL validation failed: {vr.error_message}")
            return SqlExecutionResult(
                elapsed_ms=0.0,
                validation_error=vr.error_message,
            )

    # Execute the query
    t0 = time.perf_counter()
    try:
        df = await run_sql(sql)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        return SqlExecutionResult(df=df, elapsed_ms=elapsed_ms)
    except QueryError as e:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        return SqlExecutionResult(elapsed_ms=elapsed_ms, error=str(e))
    except Exception as e:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.error(f"Unexpected SQL error:\n{traceback.format_exc()}")
        return SqlExecutionResult(elapsed_ms=elapsed_ms, error=str(e))


# ---------------------------------------------------------------------------
# Tool execution result
# ---------------------------------------------------------------------------


@dataclass
class ToolResult:
    """Result of executing a tool call."""

    result_text: str  # Text sent back to the LLM
    result_html: str | None = None  # HTML for UI display (table or chart)
    meta: dict[str, Any] = field(default_factory=dict)  # Timing / row info
    df: pd.DataFrame | None = None  # Raw dataframe for CLI output
    plotly_spec: dict[str, Any] | None = None  # Resolved Plotly spec for export


def _build_tool_meta(
    tool: str,
    sql: str,
    execution_result: SqlExecutionResult,
) -> dict[str, Any]:
    """Build metadata dict for tool result."""
    meta: dict[str, Any] = {
        "tool": tool,
        "sql": sql,
        "execution_time_ms": round(execution_result.elapsed_ms, 1),
        "row_count": None,
        "column_count": None,
        "error": execution_result.error or execution_result.validation_error,
    }
    if execution_result.df is not None:
        meta["row_count"] = len(execution_result.df)
        meta["column_count"] = len(execution_result.df.columns)
    return meta


def _log_query(
    query_logger,
    session_id: str,
    user_question: str,
    tool: str,
    sql: str,
    execution_result: SqlExecutionResult,
) -> None:
    """Log a query execution if logger is available."""
    if not query_logger:
        return
    query_logger.log(
        session_id=session_id,
        user_question=user_question,
        tool=tool,
        sql=sql,
        execution_time_ms=execution_result.elapsed_ms,
        row_count=len(execution_result.df) if execution_result.df is not None else None,
        column_count=len(execution_result.df.columns) if execution_result.df is not None else None,
        error=execution_result.error or execution_result.validation_error,
    )


async def _execute_run_sql(
    input_data: dict[str, Any],
    run_sql: RunSql,
    schema_map: dict[str, set[str]] | None,
    dialect: str,
    query_logger,
    session_id: str,
    user_question: str,
) -> ToolResult:
    """Execute the run_sql tool."""
    sql = input_data.get("sql", "")

    result = await execute_sql_with_validation(sql, run_sql, schema_map, dialect)
    _log_query(query_logger, session_id, user_question, "run_sql", sql, result)
    meta = _build_tool_meta("run_sql", sql, result)

    # Handle validation error
    if result.validation_error:
        return ToolResult(
            result_text=f"SQL validation error: {result.validation_error}\n"
            "HINT: Check the schema and fix the table/column names.",
            meta=meta,
        )

    # Handle execution error
    if result.error:
        hint = sql_error_hint(result.error, dialect=dialect)
        return ToolResult(
            result_text=f"SQL error: {result.error}{hint}",
            result_html=f"<p class='sql-error'>SQL error: {_escape_html(result.error)}</p>",
            meta=meta,
        )

    # Handle empty result
    df = result.df
    if df is None or df.empty:
        return ToolResult(
            result_text="Query executed successfully. No rows returned.",
            meta=meta,
            df=df,
        )

    # Build success response
    csv = df.to_csv(index=False)
    preview = csv if len(csv) <= 500 else csv[:500] + "\n..."
    result_text = f"{preview}\n\nReturned {len(df)} rows, {len(df.columns)} columns."
    result_html = df_to_html_table(df)

    return ToolResult(
        result_text=result_text,
        result_html=result_html,
        meta=meta,
        df=df,
    )


async def _execute_visualize_data(
    input_data: dict[str, Any],
    run_sql: RunSql,
    schema_map: dict[str, set[str]] | None,
    dialect: str,
    query_logger,
    session_id: str,
    user_question: str,
) -> ToolResult:
    """Execute the visualize_data tool."""
    sql = input_data.get("sql", "")
    title = input_data.get("title", "Chart")
    plotly_spec = input_data.get("plotly_spec", {})

    result = await execute_sql_with_validation(sql, run_sql, schema_map, dialect)
    _log_query(query_logger, session_id, user_question, "visualize_data", sql, result)
    meta = _build_tool_meta("visualize_data", sql, result)

    # Handle validation error
    if result.validation_error:
        return ToolResult(
            result_text=f"SQL validation error: {result.validation_error}\n"
            "HINT: Check the schema and fix the table/column names.",
            meta=meta,
        )

    # Handle execution error
    if result.error:
        hint = sql_error_hint(result.error, dialect=dialect)
        return ToolResult(
            result_text=f"Visualization error: {result.error}{hint}",
            result_html=f"<p class='sql-error'>Visualization error: {_escape_html(result.error)}</p>",
            meta=meta,
        )

    # Handle empty result
    df = result.df
    if df is None or df.empty:
        return ToolResult(
            result_text="Query returned no rows — nothing to visualize.",
            meta=meta,
            df=df,
        )

    # Build chart
    try:
        resolved = resolve_plotly_spec(plotly_spec, df)
        layout = resolved.get("layout", {})
        if "title" not in layout:
            layout["title"] = title
            resolved["layout"] = layout
        chart_html = build_chart_html(resolved, title)
        result_text = f"Created chart: {title} ({len(df)} rows, {len(df.columns)} columns)."

        return ToolResult(
            result_text=result_text,
            result_html=chart_html,
            meta=meta,
            df=df,
            plotly_spec=resolved,
        )
    except Exception as e:
        logger.error(f"Chart building error:\n{traceback.format_exc()}")
        return ToolResult(
            result_text=f"Chart building error: {e}",
            result_html=f"<p class='sql-error'>Chart error: {_escape_html(str(e))}</p>",
            meta=meta,
            df=df,
        )


async def execute_tool(
    name: str,
    input_data: dict[str, Any],
    *,
    run_sql: RunSql,
    schema_map: dict[str, set[str]] | None = None,
    dialect: str = "duckdb",
    query_logger=None,
    session_id: str = "",
    user_question: str = "",
) -> ToolResult:
    """Execute a tool call and return structured results.

    Parameters
    ----------
    name:
        Tool name ("run_sql" or "visualize_data").
    input_data:
        Tool input parameters.
    run_sql:
        Async function that executes SQL and returns a DataFrame.
    schema_map:
        Optional mapping of table names to column names for validation.
    dialect:
        SQL dialect for validation and error hints.
    query_logger:
        Optional query logger instance.
    session_id:
        Session ID for logging.
    user_question:
        User question for logging context.

    Returns
    -------
    ToolResult with execution results and metadata.
    """
    match name:
        case "run_sql":
            return await _execute_run_sql(
                input_data, run_sql, schema_map, dialect, query_logger, session_id, user_question
            )
        case "visualize_data":
            return await _execute_visualize_data(
                input_data, run_sql, schema_map, dialect, query_logger, session_id, user_question
            )
        case _:
            return ToolResult(result_text=f"Unknown tool: {name}", meta={})


# ---------------------------------------------------------------------------
# Agent loop
# ---------------------------------------------------------------------------


@dataclass
class AgentResult:
    """Final result of running the agent loop."""

    text: str  # Final assistant response text
    suggestions: list[str] = field(default_factory=list)
    tool_results: list[ToolResult] = field(default_factory=list)
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    api_calls: int = 0


async def run_agent_loop(
    *,
    question: str,
    llm_client: LLMClient,
    model: str,
    system_prompt: str,
    run_sql: RunSql,
    schema_map: dict[str, set[str]] | None = None,
    dialect: str = "duckdb",
    query_logger=None,
    session_id: str = "",
    messages: list[dict[str, Any]] | None = None,
    tools: list[dict[str, Any]] | None = None,
    max_iterations: int = 15,
) -> AgentResult:
    """Run the LLM agent loop to completion.

    This is a non-streaming version suitable for CLI / headless use.
    The web app uses its own streaming variant but shares the same
    ``execute_tool`` function.

    Parameters
    ----------
    question:
        User's question to answer.
    llm_client:
        LLM client for making API calls.
    model:
        Model name to use.
    system_prompt:
        System prompt with schema context.
    run_sql:
        Async function that executes SQL.
    schema_map:
        Optional schema map for validation.
    dialect:
        SQL dialect.
    query_logger:
        Optional query logger.
    session_id:
        Session ID for logging.
    messages:
        Optional existing message history.
    tools:
        Optional tool definitions (defaults to WEB_TOOLS).
    max_iterations:
        Maximum number of LLM calls.

    Returns
    -------
    AgentResult with final response and collected tool results.
    """
    if messages is None:
        messages = []
    messages.append({"role": "user", "content": question})

    if tools is None:
        tools = WEB_TOOLS

    collected_tool_results: list[ToolResult] = []
    total_input_tokens = 0
    total_output_tokens = 0
    api_calls = 0

    for _ in range(max_iterations):
        response = await llm_client.create_message(
            model=model,
            max_tokens=4096,
            system=system_prompt,
            tools=tools,
            messages=messages,
        )
        api_calls += 1
        total_input_tokens += response.usage.input_tokens
        total_output_tokens += response.usage.output_tokens

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": serialize_content(response.content)})

            tool_results_for_llm = []
            for block in response.content:
                if not isinstance(block, ToolUseBlock):
                    continue

                tool_input = dict(block.input)
                result = await execute_tool(
                    block.name,
                    tool_input,
                    run_sql=run_sql,
                    schema_map=schema_map,
                    dialect=dialect,
                    query_logger=query_logger,
                    session_id=session_id,
                    user_question=question,
                )
                collected_tool_results.append(result)
                tool_results_for_llm.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result.result_text,
                    }
                )

            messages.append({"role": "user", "content": tool_results_for_llm})
            continue

        # Final text response
        text = "".join(b.text for b in response.content if isinstance(b, TextBlock))
        suggestions = extract_suggestions(text)
        if suggestions:
            text = re.split(r"\n---\s*\n", text, maxsplit=1)[0].rstrip()

        return AgentResult(
            text=text,
            suggestions=suggestions,
            tool_results=collected_tool_results,
            total_input_tokens=total_input_tokens,
            total_output_tokens=total_output_tokens,
            api_calls=api_calls,
        )

    return AgentResult(
        text="Reached maximum number of tool calls. Please try a simpler question.",
        tool_results=collected_tool_results,
        total_input_tokens=total_input_tokens,
        total_output_tokens=total_output_tokens,
        api_calls=api_calls,
    )
