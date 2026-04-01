"""
FastAPI web application for datasight.

Provides a chat UI that streams LLM responses via SSE, with a sidebar
showing database tables and example queries. Supports Anthropic and
Ollama LLM backends via a common abstraction layer.
"""

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger

from datasight.chart import _build_artifact_html
from datasight.config import (
    create_sql_runner,
    load_schema_description,
    load_example_queries,
    format_example_queries,
)
from datasight.llm import (
    LLMClient,
    TextBlock,
    ToolUseBlock,
    create_llm_client,
)
from datasight.query_log import QueryLogger
from datasight.schema import introspect_schema, format_schema_context

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(application: FastAPI):  # noqa: ARG001
    await _startup()
    yield


app = FastAPI(title="datasight", lifespan=lifespan)

_BASE_DIR = Path(__file__).resolve().parent
_INDEX_HTML = _BASE_DIR / "templates" / "index.html"
_STATIC_DIR = _BASE_DIR / "static"

app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

# Runtime state (populated on startup)
llm_client: LLMClient | None = None
sql_runner: Any = None
system_prompt: str = ""
model: str = "claude-sonnet-4-20250514"
sessions: dict[str, list[dict[str, Any]]] = {}
schema_info: list[dict[str, Any]] = []
example_queries_list: list[dict[str, str]] = []
query_logger: QueryLogger | None = None

TOOLS: list[dict[str, Any]] = [
    {
        "name": "run_sql",
        "description": (
            "Execute a SQL query against the database and return results as a table. "
            "Use DuckDB SQL syntax. Always use this tool instead of writing SQL inline."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "SQL SELECT query to execute",
                }
            },
            "required": ["sql"],
        },
    },
    {
        "name": "visualize_data",
        "description": (
            "Execute a SQL query and render the results as an interactive Plotly.js chart. "
            "You provide a full Plotly spec with 'data' (array of trace objects) and "
            "'layout' (layout object). This supports ANY Plotly.js chart type: bar, line, "
            "scatter, pie, choropleth maps, scattergeo, treemaps, sunburst, sankey diagrams, "
            "funnel, 3D scatter/surface, candlestick, waterfall, parallel coordinates, "
            "heatmap, histogram, box, violin, and more. "
            "Use column names from your SQL query as placeholders in the spec — string values "
            "matching column names will be replaced with actual data arrays from the query results. "
            'For example: "locations": "state_code" becomes "locations": ["CA", "TX", ...]. '
            'For literal values that happen to match a column name, wrap them: {"literal": "value"}. '
            "The layout object is passed through as-is to Plotly. "
            "Use the SAME or similar SQL query that you used with run_sql. "
            "Do NOT embed raw data values in the SQL — always query the database tables."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "SQL SELECT query to fetch data for visualization",
                },
                "title": {
                    "type": "string",
                    "description": "Chart title",
                },
                "plotly_spec": {
                    "type": "object",
                    "description": (
                        "A Plotly.js specification with 'data' and 'layout' keys. "
                        "In 'data' traces, string values matching column names from the SQL query "
                        "will be replaced with arrays of actual values. "
                        "Example bar chart: "
                        '{"data": [{"type": "bar", "x": "category", "y": "total"}], '
                        '"layout": {}}. '
                        "Example choropleth: "
                        '{"data": [{"type": "choropleth", "locationmode": "USA-states", '
                        '"locations": "state_code", "z": "total_mwh", '
                        '"colorscale": {"literal": "Viridis"}}], '
                        '"layout": {"geo": {"scope": "usa"}}}'
                    ),
                    "properties": {
                        "data": {
                            "type": "array",
                            "description": "Array of Plotly trace objects",
                        },
                        "layout": {
                            "type": "object",
                            "description": "Plotly layout object",
                        },
                    },
                    "required": ["data"],
                },
            },
            "required": ["sql", "plotly_spec"],
        },
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _df_to_html_table(df: pd.DataFrame, max_rows: int = 200) -> str:
    """Render a DataFrame as a styled HTML table string."""
    if df.empty:
        return "<p class='empty-result'>Query returned no rows.</p>"

    display_df = df.head(max_rows)

    html = f"<div class='result-table-wrap' data-total-rows='{len(df)}'>"
    html += (
        "<div class='table-toolbar'>"
        "<input class='table-filter' placeholder='Filter rows...' oninput='filterTable(this)'>"
        "<button class='export-csv-btn' onclick='exportTableCsv(this)'>Download CSV</button>"
        "</div>"
    )
    html += "<table class='result-table'><thead><tr>"
    for i, col in enumerate(display_df.columns):
        html += f"<th data-col='{i}' onclick='sortTable(this)'>{col}<span class='sort-arrow'></span></th>"
    html += "</tr></thead><tbody>"
    for _, row in display_df.iterrows():
        html += "<tr>"
        for col in display_df.columns:
            val = row[col]
            if pd.isna(val):
                html += "<td class='null-val'>NULL</td>"
            else:
                html += f"<td>{val}</td>"
        html += "</tr>"
    html += "</tbody></table>"
    html += "<div class='table-pagination'></div>"
    html += "</div>"
    return html


def _sql_error_hint(error_msg: str) -> str:
    """Return a short hint to help the model fix common SQL errors."""
    hints: list[str] = []
    lower = error_msg.lower()
    if "not found in from clause" in lower or "referenced column" in lower:
        hints.append(
            "HINT: The column you referenced does not exist in the table(s) in "
            "your FROM clause. Check the schema to see which table has this "
            "column and add a JOIN if needed. For example, 'state' is in the "
            "plants table, not generation_fuel."
        )
    if "ambiguous" in lower:
        hints.append("HINT: Qualify the column with a table alias (e.g. p.state, g.report_date).")
    if "scalar function" in lower and "does not exist" in lower:
        hints.append(
            "HINT: This is DuckDB, not PostgreSQL or MySQL. Use DuckDB date functions: "
            "DATE_TRUNC('month', col), EXTRACT(YEAR FROM col), STRFTIME(col, '%Y-%m'), "
            "col::DATE. Do NOT use TO_DATE, DATE_FORMAT, STR_TO_DATE, or TO_CHAR."
        )
    if "table with name" in lower and "does not exist" in lower:
        hints.append(
            "HINT: Only these tables exist: generation_fuel, plants, plant_details "
            "(and their views). CTEs and subquery aliases are not real tables — "
            "you must include the full query, not reference a previous CTE by name."
        )
    if "syntax error" in lower:
        hints.append(
            "HINT: Write a plain SQL SELECT query against the database tables. "
            "Do not embed raw data, XML tags, or backslash line continuations in SQL. "
            "Do not use <tool_response> or similar tags — just write a normal SELECT."
        )
    return ("\n" + "\n".join(hints)) if hints else ""


def _coerce_dates(df: pd.DataFrame) -> pd.DataFrame:
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


def _resolve_plotly_spec(spec: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
    """Replace column name references in a Plotly spec with actual data arrays.

    In trace objects within spec["data"], any string value that matches a column
    name in the DataFrame (including aliased names like 'g.col' or 'g_col') is
    replaced with the list of values from that column. Objects wrapped as
    {"literal": value} are unwrapped and passed through as-is. The "layout" key
    is passed through unchanged. Date columns are coerced to ISO strings for
    JSON serialization.
    """
    df = _coerce_dates(df)
    columns = set(df.columns)

    def _resolve_value(val: Any) -> Any:
        if isinstance(val, dict):
            # {"literal": X} -> X (escape hatch for values that happen to match column names)
            if "literal" in val and len(val) == 1:
                return val["literal"]
            return {k: _resolve_value(v) for k, v in val.items()}
        if isinstance(val, list):
            return [_resolve_value(item) for item in val]
        if isinstance(val, str) and val in columns:
            series = df[val]
            # Convert datetimes to ISO strings for JSON
            if pd.api.types.is_datetime64_any_dtype(series):
                return series.dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
            return series.tolist()
        return val

    resolved: dict[str, Any] = {}
    # Resolve column references in data traces
    if "data" in spec:
        resolved["data"] = []
        for trace in spec["data"]:
            resolved_trace = {}
            for key, val in trace.items():
                # "type" should never be resolved as a column name
                if key == "type":
                    resolved_trace[key] = val
                else:
                    resolved_trace[key] = _resolve_value(val)
            resolved["data"].append(resolved_trace)
    # Pass layout through as-is
    if "layout" in spec:
        resolved["layout"] = spec["layout"]
    return resolved


async def execute_tool(
    name: str,
    input_data: dict[str, Any],
    *,
    session_id: str = "",
    user_question: str = "",
) -> tuple[str, str | None, str | None, dict[str, Any]]:
    """Execute a tool call.

    Returns (result_text_for_llm, optional_html_for_ui, optional_chart_html, meta).
    *meta* carries timing and result info for the query history panel.
    """
    if name == "run_sql":
        sql = input_data.get("sql", "")
        t0 = time.perf_counter()
        try:
            df = await sql_runner.run_sql(sql)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            if query_logger:
                query_logger.log(
                    session_id=session_id,
                    user_question=user_question,
                    tool=name,
                    sql=sql,
                    execution_time_ms=elapsed_ms,
                    row_count=len(df),
                    column_count=len(df.columns),
                )
            meta = {
                "tool": name,
                "sql": sql,
                "execution_time_ms": round(elapsed_ms, 1),
                "row_count": len(df),
                "column_count": len(df.columns),
                "error": None,
            }
            if df.empty:
                meta["row_count"] = 0
                return "Query executed successfully. No rows returned.", None, None, meta
            csv = df.to_csv(index=False)
            preview = csv if len(csv) <= 1000 else csv[:1000] + "\n(truncated)"
            result_text = f"{preview}\n\nReturned {len(df)} rows, {len(df.columns)} columns."
            result_html = _df_to_html_table(df)
            return result_text, result_html, None, meta
        except Exception as e:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            error = f"SQL error: {e}"
            logger.error(error)
            if query_logger:
                query_logger.log(
                    session_id=session_id,
                    user_question=user_question,
                    tool=name,
                    sql=sql,
                    execution_time_ms=elapsed_ms,
                    error=str(e),
                )
            meta = {
                "tool": name,
                "sql": sql,
                "execution_time_ms": round(elapsed_ms, 1),
                "row_count": None,
                "column_count": None,
                "error": str(e),
            }
            hint = _sql_error_hint(str(e))
            return error + hint, f"<p class='sql-error'>{error}</p>", None, meta

    elif name == "visualize_data":
        sql = input_data.get("sql", "")
        title = input_data.get("title", "Chart")
        plotly_spec = input_data.get("plotly_spec", {})
        t0 = time.perf_counter()
        try:
            df = await sql_runner.run_sql(sql)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            if query_logger:
                query_logger.log(
                    session_id=session_id,
                    user_question=user_question,
                    tool=name,
                    sql=sql,
                    execution_time_ms=elapsed_ms,
                    row_count=len(df),
                    column_count=len(df.columns),
                )
            meta = {
                "tool": name,
                "sql": sql,
                "execution_time_ms": round(elapsed_ms, 1),
                "row_count": len(df),
                "column_count": len(df.columns),
                "error": None,
            }
            if df.empty:
                meta["row_count"] = 0
                return "Query returned no rows — nothing to visualize.", None, None, meta
            resolved = _resolve_plotly_spec(plotly_spec, df)
            layout = resolved.get("layout", {})
            if "title" not in layout:
                layout["title"] = title
                resolved["layout"] = layout
            chart_html = _build_artifact_html(resolved, title)
            result_text = f"Created chart: {title} ({len(df)} rows, {len(df.columns)} columns)."
            return result_text, chart_html, None, meta
        except Exception as e:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            error = f"Visualization error: {e}"
            logger.error(error)
            if query_logger:
                query_logger.log(
                    session_id=session_id,
                    user_question=user_question,
                    tool=name,
                    sql=sql,
                    execution_time_ms=elapsed_ms,
                    error=str(e),
                )
            meta = {
                "tool": name,
                "sql": sql,
                "execution_time_ms": round(elapsed_ms, 1),
                "row_count": None,
                "column_count": None,
                "error": str(e),
            }
            hint = _sql_error_hint(str(e))
            return error + hint, f"<p class='sql-error'>{error}</p>", None, meta

    return f"Unknown tool: {name}", None, None, {}


def get_session_messages(session_id: str) -> list[dict[str, Any]]:
    if session_id not in sessions:
        sessions[session_id] = []
    return sessions[session_id]


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------


async def _startup():
    global \
        llm_client, \
        sql_runner, \
        system_prompt, \
        model, \
        schema_info, \
        example_queries_list, \
        query_logger

    load_dotenv()

    llm_provider = os.environ.get("LLM_PROVIDER", "anthropic")

    if llm_provider == "ollama":
        ollama_base_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434/v1")
        model = os.environ.get("OLLAMA_MODEL", "qwen3.5:35b-a3b")
        llm_client = create_llm_client(provider="ollama", base_url=ollama_base_url)
    else:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            logger.error("ANTHROPIC_API_KEY not set")
        model = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
        llm_client = create_llm_client(provider="anthropic", api_key=api_key)

    db_mode = os.environ.get("DB_MODE", "local")
    db_path = os.environ.get("DB_PATH", "")
    flight_uri = os.environ.get("FLIGHT_SQL_URI", "grpc://localhost:31337")
    flight_token = os.environ.get("FLIGHT_SQL_TOKEN")
    flight_username = os.environ.get("FLIGHT_SQL_USERNAME")
    flight_password = os.environ.get("FLIGHT_SQL_PASSWORD")

    sql_runner = create_sql_runner(
        db_mode=db_mode,
        db_path=db_path,
        flight_uri=flight_uri,
        flight_token=flight_token,
        flight_username=flight_username,
        flight_password=flight_password,
    )

    project_dir = os.environ.get("DATASIGHT_PROJECT_DIR", ".")

    log_enabled = os.environ.get("QUERY_LOG_ENABLED", "false").lower() == "true"
    log_path = os.environ.get("QUERY_LOG_PATH", os.path.join(project_dir, "query_log.jsonl"))
    query_logger = QueryLogger(path=log_path, enabled=log_enabled)
    if log_enabled:
        logger.info(f"Query logging enabled: {log_path}")

    schema_desc_path = os.environ.get("SCHEMA_DESCRIPTION_PATH")
    example_queries_path = os.environ.get("EXAMPLE_QUERIES_PATH")

    user_desc = load_schema_description(schema_desc_path, project_dir)

    # Discover schema
    tables = await introspect_schema(sql_runner.run_sql, runner=sql_runner)
    schema_info = [
        {
            "name": t.name,
            "row_count": t.row_count,
            "columns": [
                {"name": c.name, "dtype": c.dtype, "nullable": c.nullable} for c in t.columns
            ],
        }
        for t in tables
    ]

    if tables:
        total_rows = sum(t.row_count or 0 for t in tables)
        logger.info(f"Discovered {len(tables)} tables ({total_rows:,} total rows)")
        for t in tables:
            row_str = f" ({t.row_count:,} rows)" if t.row_count else ""
            logger.info(f"  {t.name}: {len(t.columns)} columns{row_str}")
    else:
        logger.warning("No tables discovered in the database")

    schema_text = format_schema_context(tables, user_desc)

    example_queries = load_example_queries(example_queries_path, project_dir)
    example_queries_list = example_queries
    if example_queries:
        schema_text += format_example_queries(example_queries)
        logger.info(f"Loaded {len(example_queries)} example queries")

    system_prompt = (
        "You are datasight, an expert data analyst assistant. You help users "
        "explore and understand data stored in a DuckDB database by writing and "
        "executing SQL queries and creating visualizations.\n\n"
        "When a user asks a question:\n"
        "1. Think about what data would answer their question.\n"
        "2. Use the run_sql tool to query the database. A chart will be "
        "created automatically from the results.\n"
        "3. If the user wants a specific visualization, use the visualize_data tool "
        "with a Plotly.js spec. You can create ANY Plotly chart type: bar, line, scatter, "
        "pie, choropleth maps, treemaps, sunburst, sankey, funnel, 3D plots, waterfall, "
        "parallel coordinates, candlestick, heatmap, and more.\n"
        "4. Explain the results clearly.\n\n"
        "In visualize_data Plotly specs, set trace values to column name strings and they "
        "will be replaced with actual data arrays from the SQL results. "
        'Use {"literal": value} to pass values that should NOT be treated as column references.\n\n'
        "Always use the tools to execute SQL — never write SQL inline without "
        "executing it. Use DuckDB SQL syntax.\n" + schema_text
    )

    port = os.environ.get("PORT", "8084")
    logger.info(f"datasight ready (model={model}, db_mode={db_mode})")
    print(f"\n  Ready — open http://localhost:{port} in your browser\n")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse(_INDEX_HTML, media_type="text/html")


@app.get("/api/schema")
async def get_schema():
    """Return discovered database schema for the sidebar."""
    return {"tables": schema_info}


@app.get("/api/queries")
async def get_queries():
    """Return example queries."""
    return {"queries": example_queries_list}


@app.post("/api/query-log/toggle")
async def toggle_query_log():
    """Enable or disable query logging at runtime."""
    if query_logger is None:
        return {"enabled": False}
    query_logger.enabled = not query_logger.enabled
    logger.info(f"Query logging {'enabled' if query_logger.enabled else 'disabled'}")
    return {"enabled": query_logger.enabled}


@app.get("/api/query-log")
async def get_query_log(n: int = 50):
    """Return recent query log entries."""
    if query_logger is None:
        return {"entries": [], "enabled": False}
    return {"entries": query_logger.read_recent(n), "enabled": query_logger.enabled}


def _log_query_cost(api_calls: int, input_tokens: int, output_tokens: int) -> None:
    """Log token usage and estimated cost for a completed query."""
    # Sonnet 4 pricing: $3/M input, $15/M output
    input_cost = input_tokens * 3.0 / 1_000_000
    output_cost = output_tokens * 15.0 / 1_000_000
    total_cost = input_cost + output_cost
    logger.info(
        f"[tokens] QUERY TOTAL: api_calls={api_calls} "
        f"input={input_tokens} output={output_tokens} "
        f"est_cost=${total_cost:.4f} "
        f"(input=${input_cost:.4f} output=${output_cost:.4f})"
    )


@app.post("/api/chat")
async def chat(request: Request):
    body = await request.json()
    message = body.get("message", "").strip()
    session_id = body.get("session_id", "default")

    if not message:
        return StreamingResponse(
            iter(["event: done\ndata: {}\n\n"]),
            media_type="text/event-stream",
        )

    async def generate():
        messages = get_session_messages(session_id)
        messages.append({"role": "user", "content": message})

        assert llm_client is not None, "LLM client not initialised"
        max_iterations = 15

        total_input_tokens = 0
        total_output_tokens = 0
        api_calls = 0
        for _ in range(max_iterations):
            try:
                response = await llm_client.create_message(
                    model=model,
                    max_tokens=4096,
                    system=system_prompt,
                    tools=TOOLS,
                    messages=messages,
                )
            except Exception as e:
                logger.error(f"LLM API error: {e}")
                yield f"event: token\ndata: {json.dumps({'text': f'Error: {e}'})}\n\n"
                yield "event: done\ndata: {}\n\n"
                return

            api_calls += 1
            total_input_tokens += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens
            logger.info(
                f"[tokens] call={api_calls} "
                f"input={response.usage.input_tokens} output={response.usage.output_tokens} "
                f"cumulative_input={total_input_tokens} cumulative_output={total_output_tokens}"
            )

            if response.stop_reason == "tool_use":
                # Serialize assistant content for session history
                serialized = []
                for block in response.content:
                    if isinstance(block, TextBlock):
                        serialized.append({"type": "text", "text": block.text})
                    elif isinstance(block, ToolUseBlock):
                        serialized.append(
                            {
                                "type": "tool_use",
                                "id": block.id,
                                "name": block.name,
                                "input": block.input,
                            }
                        )
                messages.append({"role": "assistant", "content": serialized})

                tool_results = []
                for block in response.content:
                    if not isinstance(block, ToolUseBlock):
                        continue

                    yield f"event: tool_start\ndata: {json.dumps({'tool': block.name, 'input': block.input})}\n\n"

                    result_text, result_html, auto_chart_html, meta = await execute_tool(
                        block.name,
                        block.input,
                        session_id=session_id,
                        user_question=message,
                    )

                    if result_html:
                        is_chart = block.name == "visualize_data" and "<script" in (
                            result_html or ""
                        )
                        yield f"event: tool_result\ndata: {json.dumps({'html': result_html, 'type': 'chart' if is_chart else 'table'})}\n\n"

                    if auto_chart_html:
                        yield f"event: tool_result\ndata: {json.dumps({'html': auto_chart_html, 'type': 'chart'})}\n\n"

                    if meta:
                        yield f"event: tool_done\ndata: {json.dumps(meta)}\n\n"

                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result_text,
                        }
                    )

                messages.append({"role": "user", "content": tool_results})
                continue

            # Final text response
            text = "".join(b.text for b in response.content if isinstance(b, TextBlock))

            messages.append({"role": "assistant", "content": text})

            words = text.split(" ")
            for i, word in enumerate(words):
                chunk = word if i == 0 else " " + word
                yield f"event: token\ndata: {json.dumps({'text': chunk})}\n\n"
                await asyncio.sleep(0.015)

            _log_query_cost(api_calls, total_input_tokens, total_output_tokens)
            yield "event: done\ndata: {}\n\n"
            return

        _log_query_cost(api_calls, total_input_tokens, total_output_tokens)
        yield f"event: token\ndata: {json.dumps({'text': 'Reached maximum number of tool calls. Please try a simpler question.'})}\n\n"
        yield "event: done\ndata: {}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.post("/api/clear")
async def clear_session(request: Request):
    body = await request.json()
    session_id = body.get("session_id", "default")
    if session_id in sessions:
        del sessions[session_id]
    return {"ok": True}
