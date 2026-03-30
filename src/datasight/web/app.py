"""
FastAPI web application for datasight.

Provides a chat UI that streams LLM responses via SSE, with a sidebar
showing database tables and example queries. Uses the Anthropic SDK
directly for the LLM agent loop.
"""

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from loguru import logger

import anthropic

from datasight.chart import InteractiveChartGenerator, _build_artifact_html
from datasight.config import (
    create_sql_runner,
    load_schema_description,
    load_example_queries,
    format_example_queries,
)
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

# Runtime state (populated on startup)
client: anthropic.AsyncAnthropic | None = None
sql_runner: Any = None
system_prompt: str = ""
model: str = "claude-sonnet-4-20250514"
chart_generator = InteractiveChartGenerator()
sessions: dict[str, list[anthropic.types.MessageParam]] = {}
schema_info: list[dict[str, Any]] = []
example_queries_list: list[dict[str, str]] = []

TOOLS: list[anthropic.types.ToolParam] = [
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
            "Execute a SQL query and render the results as an interactive Plotly chart. "
            "This tool runs the query itself — pass the SQL directly. "
            "Specify chart_type, x, y, and optionally color to control the visualization. "
            "If chart_type is omitted, the system will auto-detect the best chart type."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "SQL SELECT query to visualize",
                },
                "title": {
                    "type": "string",
                    "description": "Chart title",
                },
                "chart_type": {
                    "type": "string",
                    "description": "Chart type to render",
                    "enum": [
                        "bar",
                        "horizontal_bar",
                        "line",
                        "scatter",
                        "pie",
                        "area",
                        "histogram",
                        "box",
                        "heatmap",
                    ],
                },
                "x": {
                    "type": "string",
                    "description": "Column name for x-axis",
                },
                "y": {
                    "type": "string",
                    "description": "Column name for y-axis (or value axis)",
                },
                "color": {
                    "type": "string",
                    "description": "Column name for color grouping (optional)",
                },
            },
            "required": ["sql"],
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

    truncated = len(df) > max_rows
    display_df = df.head(max_rows)

    html = "<div class='result-table-wrap'>"
    html += (
        "<div class='table-toolbar'>"
        "<input class='table-filter' placeholder='Filter rows...' oninput='filterTable(this)'>"
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
    if truncated:
        html += f"<p class='table-note'>Showing {max_rows} of {len(df)} rows</p>"
    else:
        html += f"<p class='table-note'>{len(df)} row{'s' if len(df) != 1 else ''}, {len(df.columns)} column{'s' if len(df.columns) != 1 else ''}</p>"
    html += "</div>"
    return html


async def execute_tool(name: str, input_data: dict[str, Any]) -> tuple[str, str | None]:
    """Execute a tool call. Returns (result_text_for_llm, optional_html_for_ui)."""
    if name == "run_sql":
        sql = input_data.get("sql", "")
        try:
            df = await sql_runner.run_sql(sql)
            if df.empty:
                return "Query executed successfully. No rows returned.", None
            csv = df.to_csv(index=False)
            preview = csv if len(csv) <= 2000 else csv[:2000] + "\n(truncated)"
            result_text = f"{preview}\n\nReturned {len(df)} rows, {len(df.columns)} columns."
            result_html = _df_to_html_table(df)
            return result_text, result_html
        except Exception as e:
            error = f"SQL error: {e}"
            logger.error(error)
            return error, f"<p class='sql-error'>{error}</p>"

    elif name == "visualize_data":
        sql = input_data.get("sql", "")
        title = input_data.get("title", "Chart")
        chart_type = input_data.get("chart_type")
        x = input_data.get("x")
        y = input_data.get("y")
        color = input_data.get("color")
        try:
            df = await sql_runner.run_sql(sql)
            if df.empty:
                return "Query returned no rows — nothing to visualize.", None
            if chart_type:
                chart_dict = chart_generator.generate_chart_from_spec(
                    df, chart_type=chart_type, x=x, y=y, color=color, title=title,
                )
            else:
                chart_dict = chart_generator.generate_chart(df, title)
            chart_html = _build_artifact_html(chart_dict, title)
            result_text = f"Created chart: {title} ({len(df)} rows, {len(df.columns)} columns)."
            return result_text, chart_html
        except Exception as e:
            error = f"Visualization error: {e}"
            logger.error(error)
            return error, f"<p class='sql-error'>{error}</p>"

    return f"Unknown tool: {name}", None


def get_session_messages(session_id: str) -> list[anthropic.types.MessageParam]:
    if session_id not in sessions:
        sessions[session_id] = []
    return sessions[session_id]


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------


async def _startup():
    global client, sql_runner, system_prompt, model, schema_info, example_queries_list

    load_dotenv()

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set")
    client = anthropic.AsyncAnthropic(api_key=api_key)

    model = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")

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
        "2. Use the run_sql tool to query the database.\n"
        "3. Explain the results clearly.\n"
        "4. If a visualization would help, use the visualize_data tool.\n\n"
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

        assert client is not None, "Anthropic client not initialised"
        max_iterations = 15

        total_input_tokens = 0
        total_output_tokens = 0
        api_calls = 0

        for _ in range(max_iterations):
            try:
                response = await client.messages.create(
                    model=model,
                    max_tokens=4096,
                    system=system_prompt,
                    tools=TOOLS,
                    messages=messages,
                )
            except Exception as e:
                logger.error(f"Anthropic API error: {e}")
                yield f"event: token\ndata: {json.dumps({'text': f'Error: {e}'})}\n\n"
                yield "event: done\ndata: {}\n\n"
                return

            api_calls += 1
            usage = response.usage
            total_input_tokens += usage.input_tokens
            total_output_tokens += usage.output_tokens
            logger.info(
                f"[tokens] call={api_calls} "
                f"input={usage.input_tokens} output={usage.output_tokens} "
                f"cumulative_input={total_input_tokens} cumulative_output={total_output_tokens}"
            )

            if response.stop_reason == "tool_use":
                messages.append(
                    {
                        "role": "assistant",
                        "content": [block.model_dump() for block in response.content],
                    }
                )
                tool_results = []

                for block in response.content:
                    if not isinstance(block, anthropic.types.ToolUseBlock):
                        continue

                    yield f"event: tool_start\ndata: {json.dumps({'tool': block.name, 'input': block.input})}\n\n"

                    result_text, result_html = await execute_tool(block.name, block.input)

                    if result_html:
                        is_chart = block.name == "visualize_data" and "<script" in (
                            result_html or ""
                        )
                        yield f"event: tool_result\ndata: {json.dumps({'html': result_html, 'type': 'chart' if is_chart else 'table'})}\n\n"

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
            text = "".join(
                b.text for b in response.content
                if isinstance(b, anthropic.types.TextBlock)
            )
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
