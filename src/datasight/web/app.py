"""
FastAPI web application for datasight.

Provides a chat UI that streams LLM responses via SSE, with a sidebar
showing database tables and example queries. Supports Anthropic and
Ollama LLM backends via a common abstraction layer.
"""

import asyncio
import json
import os
import re
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

# ---------------------------------------------------------------------------
# Conversation persistence
# ---------------------------------------------------------------------------


class ConversationStore:
    """Persist conversations as JSON files in a directory."""

    def __init__(self, directory: Path) -> None:
        self._dir = directory
        self._dir.mkdir(parents=True, exist_ok=True)
        # In-memory cache (loaded lazily)
        self._cache: dict[str, dict[str, Any]] = {}
        self._load_all()

    def _path(self, session_id: str) -> Path:
        return self._dir / f"{session_id}.json"

    def _load_all(self) -> None:
        for f in self._dir.glob("*.json"):
            try:
                data = json.loads(f.read_text())
                self._cache[f.stem] = data
            except (json.JSONDecodeError, OSError):
                continue

    def get(self, session_id: str) -> dict[str, Any]:
        if session_id not in self._cache:
            self._cache[session_id] = {
                "title": "Untitled",
                "messages": [],
                "events": [],
            }
        return self._cache[session_id]

    def save(self, session_id: str) -> None:
        data = self._cache.get(session_id)
        if not data:
            return
        self._path(session_id).write_text(json.dumps(data))

    def delete(self, session_id: str) -> None:
        self._cache.pop(session_id, None)
        path = self._path(session_id)
        if path.exists():
            path.unlink()

    def clear_all(self) -> None:
        for sid in list(self._cache.keys()):
            self.delete(sid)

    def list_all(self) -> list[dict[str, Any]]:
        result = []
        for sid, data in self._cache.items():
            events = data.get("events", [])
            if not events:
                continue
            result.append(
                {
                    "session_id": sid,
                    "title": data.get("title", "Untitled"),
                    "message_count": sum(1 for e in events if e["event"] == "user_message"),
                }
            )
        return result


class BookmarkStore:
    """Persist bookmarked queries as a JSON file."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._bookmarks: list[dict[str, Any]] = []
        self._next_id = 1
        if self._path.exists():
            try:
                self._bookmarks = json.loads(self._path.read_text())
                if self._bookmarks:
                    self._next_id = max(b["id"] for b in self._bookmarks) + 1
            except (json.JSONDecodeError, OSError):
                pass

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._bookmarks, indent=2))

    def list_all(self) -> list[dict[str, Any]]:
        return list(self._bookmarks)

    def add(self, sql: str, tool: str = "run_sql", name: str = "") -> dict[str, Any]:
        # Avoid duplicates
        for b in self._bookmarks:
            if b["sql"] == sql:
                return b
        bookmark = {"id": self._next_id, "sql": sql, "tool": tool, "name": name}
        self._next_id += 1
        self._bookmarks.append(bookmark)
        self._save()
        return bookmark

    def delete(self, bookmark_id: int) -> None:
        self._bookmarks = [b for b in self._bookmarks if b["id"] != bookmark_id]
        self._save()

    def clear(self) -> None:
        self._bookmarks = []
        self._next_id = 1
        self._save()


# Runtime state (populated on startup)
llm_client: LLMClient | None = None
sql_runner: Any = None
system_prompt: str = ""
model: str = "claude-sonnet-4-20250514"
conversations: ConversationStore | None = None
bookmarks: BookmarkStore | None = None
schema_info: list[dict[str, Any]] = []
example_queries_list: list[dict[str, str]] = []
query_logger: QueryLogger | None = None
confirm_sql: bool = False
explain_sql: bool = False
clarify_sql: bool = True
_schema_text: str = ""

# Pending SQL confirmations: request_id -> asyncio.Event + result
_pending_confirms: dict[str, dict[str, Any]] = {}


def _rebuild_system_prompt() -> None:
    """Rebuild the system prompt (e.g. after toggling explain_sql)."""
    global system_prompt
    base_prompt = (
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
        "executing it. Use DuckDB SQL syntax.\n"
    )
    if explain_sql:
        base_prompt += (
            "\nIMPORTANT: Before executing any SQL query, you MUST first provide a "
            "brief plain-English explanation of the query. Explain: which tables are "
            "queried, what joins are used, what filters are applied, what aggregations "
            "are performed, and what the output represents. Keep the explanation concise "
            "(2-4 sentences). This helps the user verify the query logic is correct.\n"
        )
    if clarify_sql:
        base_prompt += (
            "\nIMPORTANT — MANDATORY CLARIFICATION RULES:\n"
            "Before writing any SQL, you MUST check the user's question against these "
            "rules. If ANY rule is triggered, you MUST stop and ask the user to clarify "
            "BEFORE calling any tool. Do not make assumptions.\n\n"
            "1. **Temporal granularity**: If the question involves trends, changes, or "
            "time (phrases like 'over time', 'trend', 'growth', 'by year/month', "
            "'historically') and does NOT explicitly state the granularity (daily, "
            "weekly, monthly, quarterly, yearly), you MUST ask: 'What time granularity "
            "— monthly or yearly?' (or other options that fit the data).\n"
            "2. **Aggregation scope**: If the question says 'top', 'largest', 'biggest', "
            "'most' without specifying a count, you MUST ask: 'How many? (e.g. top 5, "
            "top 10, all)'.\n"
            "3. **Metric choice**: If 'largest', 'biggest', 'most' could refer to "
            "different numeric columns, you MUST ask which metric.\n"
            "4. **Filter boundaries**: If the question uses relative terms like 'recent', "
            "'old', 'high', 'low' without numeric thresholds or date ranges, you MUST ask "
            "for specifics.\n"
            "5. **Grouping level**: If the question references a category and multiple "
            "columns could serve as the grouping key, you MUST ask which one.\n\n"
            "When asking a clarifying question, use this EXACT format — a short question "
            "followed by options as a markdown list with bold labels:\n"
            "```\n"
            "What time granularity would you like?\n"
            "- **Monthly** — one row per month\n"
            "- **Yearly** — one row per year\n"
            "```\n"
            "Always use this list format for options. Keep descriptions short (under 10 words).\n"
            "If NONE of these rules are triggered, proceed directly with the query.\n"
        )
    system_prompt = base_prompt + _schema_text


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
    assert conversations is not None
    return conversations.get(session_id)["messages"]


async def _generate_suggestions(
    messages: list[dict[str, Any]],
) -> list[str]:
    """Ask the LLM for 2-3 follow-up question suggestions."""
    assert llm_client is not None
    # Build a compact context: just the last user message and assistant reply
    recent = [m for m in messages[-4:] if isinstance(m.get("content"), str)]
    if not recent:
        return []
    response = await llm_client.create_message(
        model=model,
        max_tokens=200,
        system=(
            "Based on the conversation, suggest 2-3 short follow-up questions the user "
            "might ask next. Return ONLY a JSON array of strings, nothing else. "
            'Example: ["What is the trend over time?", "Break this down by category"]'
        ),
        tools=[],
        messages=recent,
    )
    text = "".join(b.text for b in response.content if isinstance(b, TextBlock)).strip()
    # Parse JSON array from response
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if not match:
        return []
    return json.loads(match.group())


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
        query_logger, \
        conversations, \
        bookmarks, \
        confirm_sql, \
        explain_sql, \
        clarify_sql, \
        _schema_text

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

    conversations = ConversationStore(Path(project_dir) / ".datasight" / "conversations")
    bookmarks = BookmarkStore(Path(project_dir) / ".datasight" / "bookmarks.json")

    confirm_sql = os.environ.get("CONFIRM_SQL", "false").lower() == "true"
    explain_sql = os.environ.get("EXPLAIN_SQL", "false").lower() == "true"
    clarify_sql = os.environ.get("CLARIFY_SQL", "true").lower() == "true"

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

    _schema_text = format_schema_context(tables, user_desc)

    example_queries = load_example_queries(example_queries_path, project_dir)
    example_queries_list = example_queries
    if example_queries:
        _schema_text += format_example_queries(example_queries)
        logger.info(f"Loaded {len(example_queries)} example queries")

    _rebuild_system_prompt()

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


@app.get("/api/preview/{table_name}")
async def preview_table(table_name: str):
    """Return an HTML table preview of the first 10 rows."""
    # Validate table name against known schema to prevent SQL injection
    valid_names = {t["name"] for t in schema_info}
    if table_name not in valid_names:
        return {"html": None, "error": "Unknown table"}
    try:
        df = await sql_runner.run_sql(f'SELECT * FROM "{table_name}" LIMIT 10')
        html = _df_to_html_table(df, max_rows=10)
        return {"html": html}
    except Exception as e:
        return {"html": None, "error": str(e)}


@app.get("/api/column-stats/{table_name}/{column_name}")
async def column_stats(table_name: str, column_name: str):
    """Return basic statistics for a column."""
    valid_tables = {t["name"] for t in schema_info}
    if table_name not in valid_tables:
        return {"stats": None, "error": "Unknown table"}
    # Validate column name against schema
    table_info = next((t for t in schema_info if t["name"] == table_name), None)
    if not table_info:
        return {"stats": None, "error": "Unknown table"}
    valid_cols = {c["name"] for c in table_info["columns"]}
    if column_name not in valid_cols:
        return {"stats": None, "error": "Unknown column"}
    col_info = next(c for c in table_info["columns"] if c["name"] == column_name)
    try:
        # Build stats query based on column type
        dtype = col_info["dtype"].lower()
        is_numeric = any(
            t in dtype for t in ("int", "float", "double", "decimal", "numeric", "real")
        )
        if is_numeric:
            sql = (
                f'SELECT COUNT(DISTINCT "{column_name}") AS distinct_count, '
                f'SUM(CASE WHEN "{column_name}" IS NULL THEN 1 ELSE 0 END) AS null_count, '
                f'MIN("{column_name}") AS min_val, MAX("{column_name}") AS max_val, '
                f'ROUND(AVG("{column_name}")::NUMERIC, 2) AS avg_val '
                f'FROM "{table_name}"'
            )
        else:
            sql = (
                f'SELECT COUNT(DISTINCT "{column_name}") AS distinct_count, '
                f'SUM(CASE WHEN "{column_name}" IS NULL THEN 1 ELSE 0 END) AS null_count, '
                f'MIN("{column_name}") AS min_val, MAX("{column_name}") AS max_val '
                f'FROM "{table_name}"'
            )
        df = await sql_runner.run_sql(sql)
        row = df.iloc[0]
        stats: dict[str, Any] = {
            "distinct": int(row["distinct_count"]),
            "nulls": int(row["null_count"]),
            "min": None if pd.isna(row["min_val"]) else row["min_val"],
            "max": None if pd.isna(row["max_val"]) else row["max_val"],
        }
        if is_numeric and "avg_val" in row.index:
            stats["avg"] = None if pd.isna(row["avg_val"]) else float(row["avg_val"])
        # Convert non-serializable types
        for k, v in stats.items():
            if hasattr(v, "item"):
                stats[k] = v.item()
            elif hasattr(v, "isoformat"):
                stats[k] = str(v)
        return {"stats": stats}
    except Exception as e:
        return {"stats": None, "error": str(e)}


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


@app.get("/api/bookmarks")
async def list_bookmarks():
    """Return all bookmarked queries."""
    assert bookmarks is not None
    return {"bookmarks": bookmarks.list_all()}


@app.post("/api/bookmarks")
async def add_bookmark(request: Request):
    """Add a bookmarked query."""
    assert bookmarks is not None
    body = await request.json()
    sql = body.get("sql", "").strip()
    tool = body.get("tool", "run_sql")
    name = body.get("name", "").strip()
    if not sql:
        return {"error": "sql is required"}
    bookmark = bookmarks.add(sql, tool, name)
    return {"bookmark": bookmark}


@app.delete("/api/bookmarks/{bookmark_id}")
async def remove_bookmark(bookmark_id: int):
    """Remove a bookmarked query."""
    assert bookmarks is not None
    bookmarks.delete(bookmark_id)
    return {"ok": True}


@app.delete("/api/bookmarks")
async def clear_bookmarks():
    """Remove all bookmarks."""
    assert bookmarks is not None
    bookmarks.clear()
    return {"ok": True}


@app.get("/api/settings")
async def get_settings():
    """Return current feature toggles."""
    return {
        "confirm_sql": confirm_sql,
        "explain_sql": explain_sql,
        "clarify_sql": clarify_sql,
    }


@app.post("/api/settings")
async def update_settings(request: Request):
    """Update feature toggles."""
    global confirm_sql, explain_sql, clarify_sql, system_prompt
    body = await request.json()
    need_rebuild = False
    if "confirm_sql" in body:
        confirm_sql = bool(body["confirm_sql"])
    if "explain_sql" in body:
        old = explain_sql
        explain_sql = bool(body["explain_sql"])
        if old != explain_sql:
            need_rebuild = True
    if "clarify_sql" in body:
        old = clarify_sql
        clarify_sql = bool(body["clarify_sql"])
        if old != clarify_sql:
            need_rebuild = True
    if need_rebuild:
        _rebuild_system_prompt()
    logger.info(
        f"Settings updated: confirm_sql={confirm_sql}, "
        f"explain_sql={explain_sql}, clarify_sql={clarify_sql}"
    )
    return {"confirm_sql": confirm_sql, "explain_sql": explain_sql, "clarify_sql": clarify_sql}


@app.post("/api/sql-confirm/{request_id}")
async def sql_confirm(request_id: str, request: Request):
    """Approve, edit, or reject a pending SQL confirmation."""
    body = await request.json()
    action = body.get("action", "reject")  # approve, edit, reject
    edited_sql = body.get("sql")

    pending = _pending_confirms.get(request_id)
    if not pending:
        return {"error": "No pending confirmation with that ID"}

    pending["action"] = action
    if action == "edit" and edited_sql:
        pending["sql"] = edited_sql
    pending["event"].set()
    return {"ok": True}


@app.delete("/api/conversations")
async def clear_conversations():
    """Remove all conversations."""
    assert conversations is not None
    conversations.clear_all()
    return {"ok": True}


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


@app.get("/api/conversations")
async def list_conversations():
    """Return a list of all conversations with titles."""
    assert conversations is not None
    return {"conversations": list(reversed(conversations.list_all()))}


@app.get("/api/conversations/{session_id}")
async def get_conversation(session_id: str):
    """Return the event log for a conversation (for replay)."""
    assert conversations is not None
    data = conversations.get(session_id)
    return {"events": data["events"], "title": data.get("title", "Untitled")}


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

        # Record events for conversation replay
        assert conversations is not None
        conv = conversations.get(session_id)
        evt_log = conv["events"]
        evt_log.append({"event": "user_message", "data": {"text": message}})
        # Set title from first user message
        if conv["title"] == "Untitled":
            conv["title"] = message[:80] + ("..." if len(message) > 80 else "")
        conversations.save(session_id)

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
                evt_log.append({"event": "assistant_message", "data": {"text": f"Error: {e}"}})
                conversations.save(session_id)
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

                # Stream any text blocks (e.g. SQL explanations) before tool execution
                for block in response.content:
                    if isinstance(block, TextBlock) and block.text.strip():
                        for word_i, word in enumerate(block.text.split(" ")):
                            chunk = word if word_i == 0 else " " + word
                            yield f"event: token\ndata: {json.dumps({'text': chunk})}\n\n"
                        evt_log.append(
                            {"event": "assistant_message", "data": {"text": block.text}}
                        )
                        # Signal end of explanation text before tool results
                        yield "event: explanation_done\ndata: {}\n\n"

                tool_results = []
                for block in response.content:
                    if not isinstance(block, ToolUseBlock):
                        continue

                    tool_input = dict(block.input)

                    # SQL confirmation flow
                    if confirm_sql and block.name in ("run_sql", "visualize_data"):
                        import uuid as _uuid

                        request_id = _uuid.uuid4().hex[:12]
                        confirm_event = asyncio.Event()
                        _pending_confirms[request_id] = {
                            "event": confirm_event,
                            "action": None,
                            "sql": None,
                        }
                        confirm_data = {
                            "request_id": request_id,
                            "tool": block.name,
                            "sql": tool_input.get("sql", ""),
                        }
                        yield f"event: sql_confirm\ndata: {json.dumps(confirm_data)}\n\n"

                        # Wait for user decision (timeout after 5 minutes)
                        try:
                            await asyncio.wait_for(confirm_event.wait(), timeout=300)
                        except asyncio.TimeoutError:
                            _pending_confirms.pop(request_id, None)
                            tool_results.append(
                                {
                                    "type": "tool_result",
                                    "tool_use_id": block.id,
                                    "content": "SQL confirmation timed out.",
                                }
                            )
                            continue

                        decision = _pending_confirms.pop(request_id, {})
                        action = decision.get("action", "reject")

                        if action == "reject":
                            yield "event: sql_rejected\ndata: {}\n\n"
                            tool_results.append(
                                {
                                    "type": "tool_result",
                                    "tool_use_id": block.id,
                                    "content": "User rejected this SQL query. Ask what they'd like changed.",
                                }
                            )
                            continue
                        elif action == "edit" and decision.get("sql"):
                            tool_input["sql"] = decision["sql"]

                    tool_start_data = {"tool": block.name, "input": tool_input}
                    evt_log.append({"event": "tool_start", "data": tool_start_data})
                    yield f"event: tool_start\ndata: {json.dumps(tool_start_data)}\n\n"

                    result_text, result_html, auto_chart_html, meta = await execute_tool(
                        block.name,
                        tool_input,
                        session_id=session_id,
                        user_question=message,
                    )

                    if result_html:
                        is_chart = block.name == "visualize_data" and "<script" in (
                            result_html or ""
                        )
                        result_title = tool_input.get("title", message) if is_chart else message
                        tr_data = {
                            "html": result_html,
                            "type": "chart" if is_chart else "table",
                            "title": result_title,
                        }
                        evt_log.append({"event": "tool_result", "data": tr_data})
                        yield f"event: tool_result\ndata: {json.dumps(tr_data)}\n\n"

                    if auto_chart_html:
                        ac_data = {"html": auto_chart_html, "type": "chart"}
                        evt_log.append({"event": "tool_result", "data": ac_data})
                        yield f"event: tool_result\ndata: {json.dumps(ac_data)}\n\n"

                    if meta:
                        evt_log.append({"event": "tool_done", "data": meta})
                        yield f"event: tool_done\ndata: {json.dumps(meta)}\n\n"
                        conversations.save(session_id)

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
            evt_log.append({"event": "assistant_message", "data": {"text": text}})
            conversations.save(session_id)

            words = text.split(" ")
            for i, word in enumerate(words):
                chunk = word if i == 0 else " " + word
                yield f"event: token\ndata: {json.dumps({'text': chunk})}\n\n"
                await asyncio.sleep(0.015)

            _log_query_cost(api_calls, total_input_tokens, total_output_tokens)
            yield "event: done\ndata: {}\n\n"

            # Generate follow-up suggestions (non-blocking, best-effort)
            try:
                suggestions = await _generate_suggestions(messages)
                if suggestions:
                    evt_log.append({"event": "suggestions", "data": {"suggestions": suggestions}})
                    conversations.save(session_id)
                    yield f"event: suggestions\ndata: {json.dumps({'suggestions': suggestions})}\n\n"
            except Exception:
                pass  # suggestions are optional
            return

        _log_query_cost(api_calls, total_input_tokens, total_output_tokens)
        max_iter_text = "Reached maximum number of tool calls. Please try a simpler question."
        evt_log.append({"event": "assistant_message", "data": {"text": max_iter_text}})
        conversations.save(session_id)
        yield f"event: token\ndata: {json.dumps({'text': max_iter_text})}\n\n"
        yield "event: done\ndata: {}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.post("/api/clear")
async def clear_session(request: Request):
    body = await request.json()
    session_id = body.get("session_id", "default")
    assert conversations is not None
    conversations.delete(session_id)
    return {"ok": True}
