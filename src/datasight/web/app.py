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
import uuid
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
    serialize_content,
)
from datasight.prompts import WEB_TOOLS, build_system_prompt
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


_SESSION_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


def _validate_session_id(session_id: str) -> str:
    """Validate session_id to prevent path traversal."""
    if not _SESSION_ID_RE.match(session_id) or len(session_id) > 128:
        raise ValueError(f"Invalid session_id: {session_id!r}")
    return session_id


class ConversationStore:
    """Persist conversations as JSON files in a directory."""

    def __init__(self, directory: Path) -> None:
        self._dir = directory
        self._dir.mkdir(parents=True, exist_ok=True)
        # In-memory cache (loaded lazily)
        self._cache: dict[str, dict[str, Any]] = {}
        self._load_all()

    def _path(self, session_id: str) -> Path:
        _validate_session_id(session_id)
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


class AppState:
    """Runtime state for the datasight web application, populated on startup."""

    def __init__(self) -> None:
        self.llm_client: LLMClient | None = None
        self.sql_runner: Any = None
        self.system_prompt: str = ""
        self.model: str = "claude-haiku-4-5-20251001"
        self.conversations: ConversationStore | None = None
        self.bookmarks: BookmarkStore | None = None
        self.schema_info: list[dict[str, Any]] = []
        self.example_queries_list: list[dict[str, str]] = []
        self.query_logger: QueryLogger | None = None
        self.confirm_sql: bool = False
        self.explain_sql: bool = False
        self.clarify_sql: bool = True
        self._schema_text: str = ""
        # Pending SQL confirmations: request_id -> asyncio.Event + result
        self.pending_confirms: dict[str, dict[str, Any]] = {}

    def rebuild_system_prompt(self) -> None:
        """Rebuild the system prompt (e.g. after toggling explain_sql)."""
        self.system_prompt = build_system_prompt(
            self._schema_text,
            mode="web",
            explain_sql=self.explain_sql,
            clarify_sql=self.clarify_sql,
        )


state = AppState()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _escape_html(text: str) -> str:
    """Escape HTML special characters."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def _df_to_html_table(df: pd.DataFrame, max_rows: int = 200) -> str:
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


def _sql_error_hint(error_msg: str) -> str:
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
        hints.append(
            "HINT: DuckDB syntax — use DATE_TRUNC, EXTRACT, STRFTIME, col::DATE. "
            "Not TO_DATE, DATE_FORMAT, TO_CHAR."
        )
    if "table with name" in lower and "does not exist" in lower:
        hints.append("HINT: Table not found. Check the schema for available tables.")
    if "syntax error" in lower:
        hints.append("HINT: Write a plain SQL SELECT query. No embedded data or XML tags.")
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


def _split_traces_by_group(traces: list[dict[str, Any]], df: pd.DataFrame) -> list[dict[str, Any]]:
    """Auto-split traces when the ``name`` field references a DataFrame column.

    Weaker models often produce a single trace like
    ``{"x": "date", "y": "value", "name": "category"}`` instead of creating
    one trace per category.  When ``name`` is a column reference, this function
    splits the trace into one trace per unique value of that column, filtering
    the underlying data for each.  Traces that don't reference a column in
    ``name`` are passed through unchanged.
    """
    columns = set(df.columns)
    expanded: list[dict[str, Any]] = []

    for trace in traces:
        group_col = trace.get("name")
        if not isinstance(group_col, str) or group_col not in columns:
            expanded.append(trace)
            continue

        # Identify which other trace keys reference columns (x, y, z, text, …)
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
            # Replace column references with the filtered subset's data
            for k in col_keys:
                col_name = trace[k]
                series = sub_df[col_name]
                if pd.api.types.is_datetime64_any_dtype(series):
                    new_trace[k] = series.dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()
                else:
                    new_trace[k] = series.tolist()
            expanded.append(new_trace)

    return expanded


def _resolve_plotly_spec(spec: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
    """Replace column name references in a Plotly spec with actual data arrays.

    In trace objects within spec["data"], any string value that matches a column
    name in the DataFrame (including aliased names like 'g.col' or 'g_col') is
    replaced with the list of values from that column. Objects wrapped as
    {"literal": value} are unwrapped and passed through as-is. The "layout" key
    is passed through unchanged. Date columns are coerced to ISO strings for
    JSON serialization.

    If a trace uses a column name as its ``name`` field, the trace is
    automatically split into one trace per unique value (see
    ``_split_traces_by_group``), so weaker models don't need to construct
    multi-trace specs manually.
    """
    df = _coerce_dates(df)
    columns = set(df.columns)

    # Auto-split traces whose "name" references a grouping column
    raw_traces = spec.get("data", [])
    raw_traces = _split_traces_by_group(raw_traces, df)

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
    resolved["data"] = []
    for trace in raw_traces:
        resolved_trace = {}
        for key, val in trace.items():
            # "type" and "name" should never be resolved as column names
            if key in ("type", "name"):
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
            df = await state.sql_runner.run_sql(sql)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            if state.query_logger:
                state.query_logger.log(
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
            preview = csv if len(csv) <= 500 else csv[:500] + "\n..."
            result_text = f"{preview}\n\nReturned {len(df)} rows, {len(df.columns)} columns."
            result_html = _df_to_html_table(df)
            return result_text, result_html, None, meta
        except Exception as e:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            error = f"SQL error: {e}"
            logger.error(error)
            if state.query_logger:
                state.query_logger.log(
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
            df = await state.sql_runner.run_sql(sql)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            if state.query_logger:
                state.query_logger.log(
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
            if state.query_logger:
                state.query_logger.log(
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
    if state.conversations is None:
        raise RuntimeError("App not initialised")
    return state.conversations.get(session_id)["messages"]


_MAX_HISTORY_PAIRS = 10  # Keep last N user/assistant exchanges


def _trim_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep only recent messages to bound input token growth.

    Groups messages into logical exchanges (a user text message and everything
    that follows until the next user text message). This ensures tool_use and
    tool_result pairs are never split across the trim boundary.
    """
    # Find indices of user text messages (not tool_result arrays).
    exchange_starts: list[int] = []
    for i, msg in enumerate(messages):
        if msg["role"] == "user" and isinstance(msg["content"], str):
            exchange_starts.append(i)

    if len(exchange_starts) <= _MAX_HISTORY_PAIRS:
        return messages

    # Keep the last N exchanges
    cut = exchange_starts[-_MAX_HISTORY_PAIRS]
    return messages[cut:]


def _extract_suggestions(text: str) -> list[str]:
    """Extract follow-up suggestions from the model's response.

    Looks for a ``---`` separator followed by a JSON array of strings.
    """
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


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------


async def _startup():
    load_dotenv()

    llm_provider = os.environ.get("LLM_PROVIDER", "anthropic")

    if llm_provider == "ollama":
        ollama_base_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434/v1")
        state.model = os.environ.get("OLLAMA_MODEL", "qwen3.5:35b-a3b")
        state.llm_client = create_llm_client(provider="ollama", base_url=ollama_base_url)
    else:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            logger.error("ANTHROPIC_API_KEY not set")
        state.model = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
        anthropic_base_url = os.environ.get("ANTHROPIC_BASE_URL")
        state.llm_client = create_llm_client(
            provider="anthropic", api_key=api_key, base_url=anthropic_base_url
        )

    db_mode = os.environ.get("DB_MODE", "local")
    db_path = os.environ.get("DB_PATH", "")
    flight_uri = os.environ.get("FLIGHT_SQL_URI", "grpc://localhost:31337")
    flight_token = os.environ.get("FLIGHT_SQL_TOKEN")
    flight_username = os.environ.get("FLIGHT_SQL_USERNAME")
    flight_password = os.environ.get("FLIGHT_SQL_PASSWORD")

    state.sql_runner = create_sql_runner(
        db_mode=db_mode,
        db_path=db_path,
        flight_uri=flight_uri,
        flight_token=flight_token,
        flight_username=flight_username,
        flight_password=flight_password,
    )

    project_dir = os.environ.get("DATASIGHT_PROJECT_DIR", ".")

    state.conversations = ConversationStore(Path(project_dir) / ".datasight" / "conversations")
    state.bookmarks = BookmarkStore(Path(project_dir) / ".datasight" / "bookmarks.json")

    state.confirm_sql = os.environ.get("CONFIRM_SQL", "false").lower() == "true"
    state.explain_sql = os.environ.get("EXPLAIN_SQL", "false").lower() == "true"
    state.clarify_sql = os.environ.get("CLARIFY_SQL", "true").lower() == "true"

    log_enabled = os.environ.get("QUERY_LOG_ENABLED", "false").lower() == "true"
    log_path = os.environ.get("QUERY_LOG_PATH", os.path.join(project_dir, "query_log.jsonl"))
    state.query_logger = QueryLogger(path=log_path, enabled=log_enabled)
    if log_enabled:
        logger.info(f"Query logging enabled: {log_path}")

    schema_desc_path = os.environ.get("SCHEMA_DESCRIPTION_PATH")
    example_queries_path = os.environ.get("EXAMPLE_QUERIES_PATH")

    user_desc = load_schema_description(schema_desc_path, project_dir)

    # Discover schema
    tables = await introspect_schema(state.sql_runner.run_sql, runner=state.sql_runner)
    state.schema_info = [
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

    state._schema_text = format_schema_context(tables, user_desc)

    example_queries = load_example_queries(example_queries_path, project_dir)
    state.example_queries_list = example_queries
    if example_queries:
        state._schema_text += format_example_queries(example_queries)
        logger.info(f"Loaded {len(example_queries)} example queries")

    state.rebuild_system_prompt()

    port = os.environ.get("PORT", "8084")
    logger.info(f"datasight ready (model={state.model}, db_mode={db_mode})")
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
    return {"tables": state.schema_info}


@app.get("/api/queries")
async def get_queries():
    """Return example queries."""
    return {"queries": state.example_queries_list}


@app.get("/api/preview/{table_name}")
async def preview_table(table_name: str):
    """Return an HTML table preview of the first 10 rows."""
    # Validate table name against known schema to prevent SQL injection
    valid_names = {t["name"] for t in state.schema_info}
    if table_name not in valid_names:
        return {"html": None, "error": "Unknown table"}
    try:
        df = await state.sql_runner.run_sql(f'SELECT * FROM "{table_name}" LIMIT 10')
        html = _df_to_html_table(df, max_rows=10)
        return {"html": html}
    except Exception as e:
        return {"html": None, "error": str(e)}


@app.get("/api/column-stats/{table_name}/{column_name}")
async def column_stats(table_name: str, column_name: str):
    """Return basic statistics for a column."""
    valid_tables = {t["name"] for t in state.schema_info}
    if table_name not in valid_tables:
        return {"stats": None, "error": "Unknown table"}
    # Validate column name against schema
    table_info = next((t for t in state.schema_info if t["name"] == table_name), None)
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
        df = await state.sql_runner.run_sql(sql)
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
    if state.query_logger is None:
        return {"enabled": False}
    state.query_logger.enabled = not state.query_logger.enabled
    logger.info(f"Query logging {'enabled' if state.query_logger.enabled else 'disabled'}")
    return {"enabled": state.query_logger.enabled}


@app.get("/api/query-log")
async def get_query_log(n: int = 50):
    """Return recent query log entries."""
    if state.query_logger is None:
        return {"entries": [], "enabled": False}
    return {"entries": state.query_logger.read_recent(n), "enabled": state.query_logger.enabled}


@app.get("/api/bookmarks")
async def list_bookmarks():
    """Return all bookmarked queries."""
    if state.bookmarks is None:
        raise RuntimeError("App not initialised")
    return {"bookmarks": state.bookmarks.list_all()}


@app.post("/api/bookmarks")
async def add_bookmark(request: Request):
    """Add a bookmarked query."""
    if state.bookmarks is None:
        raise RuntimeError("App not initialised")
    body = await request.json()
    sql = body.get("sql", "").strip()
    tool = body.get("tool", "run_sql")
    name = body.get("name", "").strip()
    if not sql:
        return {"error": "sql is required"}
    bookmark = state.bookmarks.add(sql, tool, name)
    return {"bookmark": bookmark}


@app.delete("/api/bookmarks/{bookmark_id}")
async def remove_bookmark(bookmark_id: int):
    """Remove a bookmarked query."""
    if state.bookmarks is None:
        raise RuntimeError("App not initialised")
    state.bookmarks.delete(bookmark_id)
    return {"ok": True}


@app.delete("/api/bookmarks")
async def clear_bookmarks():
    """Remove all bookmarks."""
    if state.bookmarks is None:
        raise RuntimeError("App not initialised")
    state.bookmarks.clear()
    return {"ok": True}


@app.get("/api/settings")
async def get_settings():
    """Return current feature toggles."""
    return {
        "confirm_sql": state.confirm_sql,
        "explain_sql": state.explain_sql,
        "clarify_sql": state.clarify_sql,
    }


@app.post("/api/settings")
async def update_settings(request: Request):
    """Update feature toggles."""
    body = await request.json()
    need_rebuild = False
    if "confirm_sql" in body:
        state.confirm_sql = bool(body["confirm_sql"])
    if "explain_sql" in body:
        old = state.explain_sql
        state.explain_sql = bool(body["explain_sql"])
        if old != state.explain_sql:
            need_rebuild = True
    if "clarify_sql" in body:
        old = state.clarify_sql
        state.clarify_sql = bool(body["clarify_sql"])
        if old != state.clarify_sql:
            need_rebuild = True
    if need_rebuild:
        state.rebuild_system_prompt()
    logger.info(
        f"Settings updated: confirm_sql={state.confirm_sql}, "
        f"explain_sql={state.explain_sql}, clarify_sql={state.clarify_sql}"
    )
    return {
        "confirm_sql": state.confirm_sql,
        "explain_sql": state.explain_sql,
        "clarify_sql": state.clarify_sql,
    }


@app.post("/api/sql-confirm/{request_id}")
async def sql_confirm(request_id: str, request: Request):
    """Approve, edit, or reject a pending SQL confirmation."""
    body = await request.json()
    action = body.get("action", "reject")  # approve, edit, reject
    edited_sql = body.get("sql")

    pending = state.pending_confirms.get(request_id)
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
    if state.conversations is None:
        raise RuntimeError("App not initialised")
    state.conversations.clear_all()
    return {"ok": True}


# Pricing per million tokens: (input, output)
_MODEL_PRICING: dict[str, tuple[float, float]] = {
    "claude-sonnet-4-20250514": (3.0, 15.0),
    "claude-haiku-4-5-20251001": (0.80, 4.0),
    "claude-opus-4-20250514": (15.0, 75.0),
}


def _log_query_cost(api_calls: int, input_tokens: int, output_tokens: int) -> None:
    """Log token usage and estimated cost for a completed query."""
    pricing = _MODEL_PRICING.get(state.model)
    if pricing:
        input_cost = input_tokens * pricing[0] / 1_000_000
        output_cost = output_tokens * pricing[1] / 1_000_000
        total_cost = input_cost + output_cost
        cost_str = (
            f" est_cost=${total_cost:.4f} (input=${input_cost:.4f} output=${output_cost:.4f})"
        )
    else:
        cost_str = ""
    logger.info(
        f"[tokens] QUERY TOTAL: api_calls={api_calls} "
        f"input={input_tokens} output={output_tokens}{cost_str}"
    )


@app.get("/api/conversations")
async def list_conversations():
    """Return a list of all conversations with titles."""
    if state.conversations is None:
        raise RuntimeError("App not initialised")
    return {"conversations": list(reversed(state.conversations.list_all()))}


@app.get("/api/conversations/{session_id}")
async def get_conversation(session_id: str):
    """Return the event log for a conversation (for replay)."""
    if state.conversations is None:
        raise RuntimeError("App not initialised")
    data = state.conversations.get(session_id)
    return {"events": data["events"], "title": data.get("title", "Untitled")}


@app.post("/api/chat")
async def chat(request: Request):
    body = await request.json()
    message = body.get("message", "").strip()
    session_id = body.get("session_id", "default")

    try:
        _validate_session_id(session_id)
    except ValueError:
        return StreamingResponse(
            iter(['event: error\ndata: {"error":"Invalid session ID"}\n\n']),
            media_type="text/event-stream",
        )

    if not message:
        return StreamingResponse(
            iter(["event: done\ndata: {}\n\n"]),
            media_type="text/event-stream",
        )

    async def generate():
        messages = get_session_messages(session_id)
        messages.append({"role": "user", "content": message})

        # Record events for conversation replay
        if state.conversations is None:
            raise RuntimeError("App not initialised")
        conv = state.conversations.get(session_id)
        evt_log = conv["events"]
        evt_log.append({"event": "user_message", "data": {"text": message}})
        # Set title from first user message
        if conv["title"] == "Untitled":
            conv["title"] = message[:80] + ("..." if len(message) > 80 else "")
        state.conversations.save(session_id)

        if state.llm_client is None:
            raise RuntimeError("LLM client not initialised")
        max_iterations = 15

        total_input_tokens = 0
        total_output_tokens = 0
        api_calls = 0
        for _ in range(max_iterations):
            trimmed = _trim_messages(messages)
            try:
                response = await state.llm_client.create_message(
                    model=state.model,
                    max_tokens=4096,
                    system=state.system_prompt,
                    tools=WEB_TOOLS,
                    messages=trimmed,
                )
            except Exception as e:
                logger.error(f"LLM API error: {e}")
                evt_log.append({"event": "assistant_message", "data": {"text": f"Error: {e}"}})
                state.conversations.save(session_id)
                yield f"event: token\ndata: {json.dumps({'text': f'Error: {e}'})}\n\n"
                yield "event: done\ndata: {}\n\n"
                return

            api_calls += 1
            total_input_tokens += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens
            logger.info(
                f"[tokens] call={api_calls} "
                f"input={response.usage.input_tokens} output={response.usage.output_tokens} "
                f"cache_create={response.usage.cache_creation_input_tokens} "
                f"cache_read={response.usage.cache_read_input_tokens} "
                f"cumulative_input={total_input_tokens} cumulative_output={total_output_tokens}"
            )

            if response.stop_reason == "tool_use":
                # Serialize assistant content for session history
                messages.append(
                    {"role": "assistant", "content": serialize_content(response.content)}
                )

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
                    if state.confirm_sql and block.name in ("run_sql", "visualize_data"):
                        request_id = uuid.uuid4().hex[:12]
                        confirm_event = asyncio.Event()
                        state.pending_confirms[request_id] = {
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
                            state.pending_confirms.pop(request_id, None)
                            tool_results.append(
                                {
                                    "type": "tool_result",
                                    "tool_use_id": block.id,
                                    "content": "SQL confirmation timed out.",
                                }
                            )
                            continue

                        decision = state.pending_confirms.pop(request_id, {})
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
                        state.conversations.save(session_id)

                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result_text,
                        }
                    )

                messages.append({"role": "user", "content": tool_results})
                continue

            # Final text response — extract inline suggestions if present
            text = "".join(b.text for b in response.content if isinstance(b, TextBlock))
            suggestions = _extract_suggestions(text)
            if suggestions:
                # Strip the suggestions block from the visible text
                text = re.split(r"\n---\s*\n", text, maxsplit=1)[0].rstrip()

            messages.append({"role": "assistant", "content": text})
            evt_log.append({"event": "assistant_message", "data": {"text": text}})
            state.conversations.save(session_id)

            words = text.split(" ")
            for i, word in enumerate(words):
                chunk = word if i == 0 else " " + word
                yield f"event: token\ndata: {json.dumps({'text': chunk})}\n\n"
                await asyncio.sleep(0.015)

            _log_query_cost(api_calls, total_input_tokens, total_output_tokens)
            yield "event: done\ndata: {}\n\n"

            if suggestions:
                evt_log.append({"event": "suggestions", "data": {"suggestions": suggestions}})
                state.conversations.save(session_id)
                yield f"event: suggestions\ndata: {json.dumps({'suggestions': suggestions})}\n\n"
            return

        _log_query_cost(api_calls, total_input_tokens, total_output_tokens)
        max_iter_text = "Reached maximum number of tool calls. Please try a simpler question."
        evt_log.append({"event": "assistant_message", "data": {"text": max_iter_text}})
        state.conversations.save(session_id)
        yield f"event: token\ndata: {json.dumps({'text': max_iter_text})}\n\n"
        yield "event: done\ndata: {}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.post("/api/clear")
async def clear_session(request: Request):
    body = await request.json()
    session_id = body.get("session_id", "default")
    if state.conversations is None:
        raise RuntimeError("App not initialised")
    state.conversations.delete(session_id)
    return {"ok": True}
