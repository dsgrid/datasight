"""
FastAPI web application for datasight.

Provides a chat UI that streams LLM responses via SSE, with a sidebar
showing database tables and example queries. Supports Anthropic and
Ollama LLM backends via a common abstraction layer.
"""

import asyncio
import hashlib
import json
import os
import re
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger

from datasight.agent import (
    df_to_html_table,
    execute_tool,
    extract_suggestions,
)
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
from datasight.recent_projects import (
    add_recent_project,
    get_project_name,
    load_recent_projects,
    remove_recent_project,
    validate_project_dir,
)
from datasight.schema import introspect_schema, format_schema_context
from datasight.sql_validation import build_schema_map

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
        self.schema_text: str = ""
        self.schema_map: dict[str, set[str]] = {}  # lowercase table -> lowercase columns
        self.sql_dialect: str = "duckdb"  # "duckdb", "postgres", or "sqlite"
        # Project state
        self.project_dir: str | None = None  # None when no project loaded
        self.project_loaded: bool = False
        # Pending SQL confirmations: request_id -> asyncio.Event + result
        self.pending_confirms: dict[str, dict[str, Any]] = {}
        # Response cache: normalized question -> cached response data
        self._response_cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._response_cache_max = 100

    def clear_project(self) -> None:
        """Clear project-specific state."""
        if self.sql_runner:
            # Close existing connection if possible
            if hasattr(self.sql_runner, "close"):
                try:
                    self.sql_runner.close()
                except Exception:
                    pass
        self.sql_runner = None
        self.schema_info = []
        self.example_queries_list = []
        self.schema_text = ""
        self.schema_map = {}
        self.system_prompt = ""
        self.project_dir = None
        self.project_loaded = False
        self.conversations = None
        self.bookmarks = None
        self.query_logger = None
        self._response_cache.clear()

    def rebuild_system_prompt(self) -> None:
        """Rebuild the system prompt (e.g. after toggling explain_sql or clarify_sql)."""
        self.system_prompt = build_system_prompt(
            self.schema_text,
            mode="web",
            explain_sql=self.explain_sql,
            clarify_sql=self.clarify_sql,
            dialect=self.sql_dialect,
        )
        # Invalidate response cache when prompt changes
        self._response_cache.clear()

    @staticmethod
    def _cache_key(question: str) -> str:
        """Normalize a question into a cache key."""
        normalized = " ".join(question.lower().split())
        return hashlib.sha256(normalized.encode()).hexdigest()

    def cache_get(self, question: str) -> dict[str, Any] | None:
        key = self._cache_key(question)
        entry = self._response_cache.get(key)
        if entry is not None:
            self._response_cache.move_to_end(key)
        return entry

    def cache_put(self, question: str, data: dict[str, Any]) -> None:
        key = self._cache_key(question)
        self._response_cache[key] = data
        self._response_cache.move_to_end(key)
        while len(self._response_cache) > self._response_cache_max:
            self._response_cache.popitem(last=False)


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
    return df_to_html_table(df, max_rows=max_rows)


async def _execute_tool_web(
    name: str,
    input_data: dict[str, Any],
    *,
    session_id: str = "",
    user_question: str = "",
) -> tuple[str, str | None, str | None, dict[str, Any]]:
    """Execute a tool call via the shared agent module.

    Returns (result_text_for_llm, optional_html_for_ui, optional_chart_html, meta).
    """
    result = await execute_tool(
        name,
        input_data,
        run_sql=state.sql_runner.run_sql,
        schema_map=state.schema_map or None,
        dialect=state.sql_dialect,
        query_logger=state.query_logger,
        session_id=session_id,
        user_question=user_question,
    )
    return result.result_text, result.result_html, None, result.meta


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
    return extract_suggestions(text)


# ---------------------------------------------------------------------------
# Startup and Project Loading
# ---------------------------------------------------------------------------


def _reinit_llm_client() -> None:
    """Initialize or reinitialize the LLM client from current environment variables."""
    llm_provider = os.environ.get("LLM_PROVIDER", "anthropic")

    if llm_provider == "ollama":
        ollama_base_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434/v1")
        state.model = os.environ.get("OLLAMA_MODEL", "qwen3.5:35b-a3b")
        state.llm_client = create_llm_client(provider="ollama", base_url=ollama_base_url)
    elif llm_provider == "github":
        api_key = os.environ.get("GITHUB_TOKEN", "")
        if not api_key:
            logger.warning("LLM API key not configured (GITHUB_TOKEN)")
        state.model = os.environ.get("GITHUB_MODELS_MODEL", "gpt-4o")
        github_base_url = os.environ.get("GITHUB_MODELS_BASE_URL")
        state.llm_client = create_llm_client(
            provider="github", api_key=api_key, base_url=github_base_url
        )
    else:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            logger.warning("LLM API key not configured (ANTHROPIC_API_KEY)")
        state.model = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
        anthropic_base_url = os.environ.get("ANTHROPIC_BASE_URL")
        state.llm_client = create_llm_client(
            provider="anthropic", api_key=api_key, base_url=anthropic_base_url
        )


async def _startup():
    """Initialize the LLM client. Projects are loaded separately via the UI."""
    load_dotenv()

    _reinit_llm_client()

    # Check for auto-load project (from CLI --project-dir)
    auto_load_project = os.environ.get("DATASIGHT_AUTO_LOAD_PROJECT")
    if auto_load_project:
        try:
            await load_project(auto_load_project)
            logger.info(f"Auto-loaded project: {auto_load_project}")
        except Exception as e:
            logger.error(f"Failed to auto-load project {auto_load_project}: {e}")

    port = os.environ.get("PORT", "8084")
    if state.project_loaded:
        logger.info(f"datasight ready (model={state.model}, project={state.project_dir})")
    else:
        logger.info(f"datasight ready (model={state.model}, no project loaded)")
    print(f"\n  Ready — open http://localhost:{port} in your browser\n")


async def load_project(project_dir: str) -> dict[str, Any]:
    """Load a project directory, initializing DB connection and schema.

    This can be called to switch projects at runtime.
    Returns project info on success, raises exception on failure.
    """
    from datasight.config import normalize_db_mode

    project_dir = str(Path(project_dir).resolve())

    # Validate the project directory
    is_valid, error = validate_project_dir(project_dir)
    if not is_valid:
        raise ValueError(error)

    # Clear any existing project state
    state.clear_project()

    # Load .env from the project directory
    env_path = os.path.join(project_dir, ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path, override=True)

    # Reinitialize LLM client in case the project has different/new API keys
    _reinit_llm_client()

    # Set up database connection based on project's .env
    db_mode = normalize_db_mode(os.environ.get("DB_MODE", "duckdb"))
    db_path = os.environ.get("DB_PATH", "database.duckdb")
    flight_uri = os.environ.get("FLIGHT_SQL_URI", "grpc://localhost:31337")
    flight_token = os.environ.get("FLIGHT_SQL_TOKEN")
    flight_username = os.environ.get("FLIGHT_SQL_USERNAME")
    flight_password = os.environ.get("FLIGHT_SQL_PASSWORD")

    # Resolve relative DB path
    if db_mode in ("duckdb", "sqlite") and not os.path.isabs(db_path):
        db_path = str(Path(project_dir) / db_path)

    # Validate DB file exists for file-based databases
    if db_mode in ("duckdb", "sqlite") and not os.path.exists(db_path):
        raise ValueError(f"Database file not found: {db_path}")

    # Map DB mode to SQL dialect
    _DB_MODE_DIALECTS = {"duckdb": "duckdb", "sqlite": "sqlite", "postgres": "postgres"}
    state.sql_dialect = _DB_MODE_DIALECTS.get(db_mode, "duckdb")

    state.sql_runner = create_sql_runner(
        db_mode=db_mode,
        db_path=db_path,
        flight_uri=flight_uri,
        flight_token=flight_token,
        flight_username=flight_username,
        flight_password=flight_password,
        postgres_host=os.environ.get("POSTGRES_HOST", "localhost"),
        postgres_port=int(os.environ.get("POSTGRES_PORT", "5432")),
        postgres_database=os.environ.get("POSTGRES_DATABASE", ""),
        postgres_user=os.environ.get("POSTGRES_USER", ""),
        postgres_password=os.environ.get("POSTGRES_PASSWORD", ""),
        postgres_url=os.environ.get("POSTGRES_URL", ""),
        postgres_sslmode=os.environ.get("POSTGRES_SSLMODE", "prefer"),
    )

    state.project_dir = project_dir

    # Track this project in recent projects list
    add_recent_project(project_dir)

    # Set up project-specific storage
    state.conversations = ConversationStore(Path(project_dir) / ".datasight" / "conversations")
    state.bookmarks = BookmarkStore(Path(project_dir) / ".datasight" / "bookmarks.json")

    # Load settings from env
    state.confirm_sql = os.environ.get("CONFIRM_SQL", "false").lower() == "true"
    state.explain_sql = os.environ.get("EXPLAIN_SQL", "false").lower() == "true"
    state.clarify_sql = os.environ.get("CLARIFY_SQL", "true").lower() == "true"

    log_enabled = os.environ.get("QUERY_LOG_ENABLED", "false").lower() == "true"
    log_path = os.environ.get("QUERY_LOG_PATH", os.path.join(project_dir, "query_log.jsonl"))
    state.query_logger = QueryLogger(path=log_path, enabled=log_enabled)

    # Load schema description and example queries
    schema_desc_path = os.environ.get("SCHEMA_DESCRIPTION_PATH")
    example_queries_path = os.environ.get("EXAMPLE_QUERIES_PATH")
    user_desc = load_schema_description(schema_desc_path, project_dir)

    # Discover schema from database
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
    else:
        logger.warning("No tables discovered in the database")

    state.schema_text = format_schema_context(tables, user_desc)
    state.schema_map = build_schema_map(state.schema_info)

    example_queries = load_example_queries(example_queries_path, project_dir)
    state.example_queries_list = example_queries
    if example_queries:
        state.schema_text += format_example_queries(example_queries)
        logger.info(f"Loaded {len(example_queries)} example queries")

    state.rebuild_system_prompt()
    state.project_loaded = True

    return {
        "path": project_dir,
        "name": get_project_name(project_dir),
        "tables": len(state.schema_info),
        "queries": len(state.example_queries_list),
    }


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


# ---------------------------------------------------------------------------
# Project management endpoints
# ---------------------------------------------------------------------------


@app.get("/api/project")
async def get_current_project():
    """Return current project info, or null if no project is loaded."""
    if not state.project_loaded or state.project_dir is None:
        return {"loaded": False, "path": None, "name": None}
    return {
        "loaded": True,
        "path": state.project_dir,
        "name": get_project_name(state.project_dir),
    }


@app.get("/api/projects/recent")
async def get_recent_projects():
    """Return list of recent projects."""
    projects = load_recent_projects()
    return {
        "projects": [
            {
                "path": p["path"],
                "name": get_project_name(p["path"]),
                "last_used": p.get("last_used", ""),
                "is_current": state.project_loaded and p["path"] == state.project_dir,
            }
            for p in projects
        ]
    }


@app.post("/api/projects/validate")
async def validate_project_endpoint(request: Request):
    """Validate a project directory before loading."""
    body = await request.json()
    project_path = body.get("path", "")
    is_valid, error = validate_project_dir(project_path)
    return {"valid": is_valid, "error": error}


@app.post("/api/projects/load")
async def load_project_endpoint(request: Request):
    """Load a project directory.

    This initializes the database connection, loads schema, and sets up
    project-specific state. Can be called to switch between projects.
    """
    body = await request.json()
    project_path = body.get("path", "")

    try:
        result = await load_project(project_path)
        return {"success": True, **result}
    except Exception as e:
        logger.error(f"Failed to load project {project_path}: {e}")
        return {"success": False, "error": str(e)}


@app.delete("/api/projects/recent/{project_path:path}")
async def remove_project_from_recent(project_path: str):
    """Remove a project from the recent list."""
    # URL decode the path (FastAPI handles this, but path may have been encoded)
    from urllib.parse import unquote

    decoded_path = unquote(project_path)
    # Handle paths that start with / (absolute paths)
    if not decoded_path.startswith("/"):
        decoded_path = "/" + decoded_path
    remove_recent_project(decoded_path)
    return {"success": True}


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
            if state.sql_dialect == "sqlite":
                avg_expr = f'ROUND(AVG("{column_name}"), 2)'
            elif state.sql_dialect == "postgres":
                avg_expr = f'ROUND(AVG("{column_name}")::NUMERIC, 2)'
            else:
                avg_expr = f'ROUND(AVG("{column_name}")::NUMERIC, 2)'
            sql = (
                f'SELECT COUNT(DISTINCT "{column_name}") AS distinct_count, '
                f'SUM(CASE WHEN "{column_name}" IS NULL THEN 1 ELSE 0 END) AS null_count, '
                f'MIN("{column_name}") AS min_val, MAX("{column_name}") AS max_val, '
                f"{avg_expr} AS avg_val "
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

        is_first_turn = len(messages) == 1

        # Check response cache for first-turn questions
        if is_first_turn:
            cached = state.cache_get(message)
            if cached is not None:
                logger.info(f"[cache] HIT for question: {message[:60]}")
                # Replay cached events into conversation and stream
                for evt in cached["events"]:
                    evt_log.append(evt)
                    yield f"event: {evt['event']}\ndata: {json.dumps(evt['data'])}\n\n"
                # Restore messages for session history
                for msg in cached["messages"]:
                    messages.append(msg)
                state.conversations.save(session_id)
                yield "event: done\ndata: {}\n\n"
                if cached.get("suggestions"):
                    evt_log.append(
                        {"event": "suggestions", "data": {"suggestions": cached["suggestions"]}}
                    )
                    state.conversations.save(session_id)
                    yield f"event: suggestions\ndata: {json.dumps({'suggestions': cached['suggestions']})}\n\n"
                return

        # Track starting positions for cache collection
        _evt_start = len(evt_log)
        _msg_start = len(messages)

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

                    result_text, result_html, auto_chart_html, meta = await _execute_tool_web(
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

            # Cache first-turn responses for identical future questions
            if is_first_turn:
                new_events = evt_log[_evt_start:]
                new_messages = messages[_msg_start:]
                state.cache_put(
                    message,
                    {
                        "events": [e for e in new_events],
                        "messages": [m for m in new_messages],
                        "suggestions": suggestions,
                    },
                )
                logger.info(f"[cache] STORED for question: {message[:60]}")

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


@app.post("/api/export/{session_id}")
async def export_session(session_id: str, request: Request):
    """Export a conversation as self-contained HTML."""
    from datasight.export import export_session_html

    if state.conversations is None:
        raise RuntimeError("App not initialised")
    body = await request.json()
    exclude = body.get("exclude_indices", [])
    exclude_set = set(exclude) if exclude else None

    data = state.conversations.get(session_id)
    events = data.get("events", [])
    title = data.get("title", "datasight session")

    html = export_session_html(events, title=title, exclude_indices=exclude_set)
    return HTMLResponse(
        content=html,
        headers={
            "Content-Disposition": 'attachment; filename="datasight-export.html"',
        },
    )
