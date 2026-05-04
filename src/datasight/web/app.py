"""
FastAPI web application for datasight.

Provides a chat UI that streams LLM responses via SSE, with a sidebar
showing database tables and example queries. Supports Anthropic and
Ollama LLM backends via a common abstraction layer.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Any, AsyncIterator

import pandas as pd
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from loguru import logger

from datasight.agent import df_to_html_table, execute_tool, extract_suggestions
from datasight.config import (
    create_sql_runner_from_settings,
    format_example_queries,
    load_example_queries,
    load_measure_overrides,
    load_schema_config,
    load_schema_description,
    load_time_series_config,
)
import yaml
from datasight.data_profile import (
    build_dataset_overview,
    build_dimension_overview,
    build_measure_overview,
    find_table_info,
    build_time_series_quality,
    format_measure_overrides_yaml,
    format_measure_prompt_context,
    format_time_series_prompt_context,
    build_prompt_recipes,
    build_quality_overview,
    build_trend_overview,
)
from datasight.events import EventType
from datasight.cost import build_cost_data, log_query_cost
from datasight.exceptions import (
    ConfigurationError,
    InvalidSessionIdError,
    LLMError,
    ProjectError,
)
from datasight.llm import LLMClient, TextBlock, ToolUseBlock, create_llm_client, serialize_content
from datasight.prompts import WEB_TOOLS, build_system_prompt
from datasight.query_log import QueryLogger
from datasight.recent_projects import (
    add_recent_project,
    get_project_name,
    load_recent_projects,
    remove_recent_project,
    validate_project_dir,
)
from datasight.explore import (
    add_files_to_connection,
    save_ephemeral_as_project,
    scan_directory_for_data_files,
)
from datasight.generate import (
    build_generation_context,
    parse_generation_response,
    sample_enum_columns,
    sample_timestamp_columns,
)
from datasight.runner import CachingSqlRunner, SqlRunner
from datasight.schema import filter_tables, format_schema_context, introspect_schema
from datasight.schema_links import resolve_schema_description_links
from datasight.settings import (
    Settings,
    capture_original_env,
    load_global_env,
    restore_original_env,
)
from datasight.sql_validation import build_measure_rule_map, build_schema_map, validate_sql

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(application: FastAPI):  # noqa: ARG001
    await _startup()
    yield


def _sanitize_non_finite(value: Any) -> Any:
    # Starlette's JSONResponse encodes with allow_nan=False, which rejects NaN
    # and ±Inf. Persisted event logs (written with default json.dumps) and
    # pandas-derived payloads can carry these values into responses, so we
    # replace them with None before encoding.
    if isinstance(value, dict):
        return {k: _sanitize_non_finite(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_non_finite(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if pd.isna(value):
        return None
    return value


class SafeJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return json.dumps(
            _sanitize_non_finite(content),
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
        ).encode("utf-8")


app = FastAPI(
    title="datasight",
    lifespan=lifespan,
    default_response_class=SafeJSONResponse,
)

_BASE_DIR = Path(__file__).resolve().parent
_INDEX_HTML = _BASE_DIR / "templates" / "index.html"
_STATIC_DIR = _BASE_DIR / "static"

app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


# ---------------------------------------------------------------------------
# Session ID validation
# ---------------------------------------------------------------------------


_SESSION_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


def validate_session_id(session_id: str) -> str:
    """Validate session_id to prevent path traversal.

    Raises
    ------
    InvalidSessionIdError:
        If the session ID contains invalid characters or is too long.
    """
    if not _SESSION_ID_RE.match(session_id) or len(session_id) > 128:
        raise InvalidSessionIdError(f"Invalid session_id: {session_id!r}")
    return session_id


# ---------------------------------------------------------------------------
# Conversation persistence
# ---------------------------------------------------------------------------


class ConversationStore:
    """Persist conversations as JSON files in a directory."""

    def __init__(self, directory: Path) -> None:
        self._dir = directory
        self._dir.mkdir(parents=True, exist_ok=True)
        self._cache: dict[str, dict[str, Any]] = {}
        self._load_all()

    def _path(self, session_id: str) -> Path:
        validate_session_id(session_id)
        return self._dir / f"{session_id}.json"

    def _load_all(self) -> None:
        for f in self._dir.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
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
        self._path(session_id).write_text(
            json.dumps(_sanitize_non_finite(data), allow_nan=False),
            encoding="utf-8",
        )

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
                    "message_count": sum(
                        1 for e in events if e["event"] == EventType.USER_MESSAGE
                    ),
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
                self._bookmarks = json.loads(self._path.read_text(encoding="utf-8"))
                if self._bookmarks:
                    self._next_id = max(b["id"] for b in self._bookmarks) + 1
            except (json.JSONDecodeError, OSError):
                pass

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._bookmarks, indent=2), encoding="utf-8")

    def list_all(self) -> list[dict[str, Any]]:
        return list(self._bookmarks)

    def add(self, sql: str, tool: str = "run_sql", name: str = "") -> dict[str, Any]:
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


class ReportStore:
    """Persist saved reports (rerunnable query + visualization) as a JSON file."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._reports: list[dict[str, Any]] = []
        self._next_id = 1
        if self._path.exists():
            try:
                self._reports = json.loads(self._path.read_text(encoding="utf-8"))
                if self._reports:
                    self._next_id = max(r["id"] for r in self._reports) + 1
            except (json.JSONDecodeError, OSError):
                pass

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._reports, indent=2), encoding="utf-8")

    def list_all(self) -> list[dict[str, Any]]:
        return list(self._reports)

    def get(self, report_id: int) -> dict[str, Any] | None:
        for r in self._reports:
            if r["id"] == report_id:
                return dict(r)
        return None

    def add(
        self,
        sql: str,
        tool: str = "run_sql",
        name: str = "",
        plotly_spec: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        report: dict[str, Any] = {
            "id": self._next_id,
            "sql": sql,
            "tool": tool,
            "name": name,
        }
        if plotly_spec is not None:
            report["plotly_spec"] = plotly_spec
        self._next_id += 1
        self._reports.append(report)
        self._save()
        return report

    def update(self, report_id: int, fields: dict[str, Any]) -> dict[str, Any] | None:
        for r in self._reports:
            if r["id"] == report_id:
                for key in ("sql", "name", "plotly_spec"):
                    if key in fields:
                        r[key] = fields[key]
                self._save()
                return dict(r)
        return None

    def delete(self, report_id: int) -> None:
        self._reports = [r for r in self._reports if r["id"] != report_id]
        self._save()

    def clear(self) -> None:
        self._reports = []
        self._next_id = 1
        self._save()


class DashboardStore:
    """Persist dashboard items as a JSON file."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._items: list[dict[str, Any]] = []
        self._columns: int = 0
        self._filters: list[dict[str, Any]] = []
        self._title: str = ""
        self._next_id = 1
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text(encoding="utf-8"))
                self._items = data.get("items", [])
                self._columns = data.get("columns", 0)
                self._filters = data.get("filters", [])
                self._title = data.get("title", "") or ""
                if self._items:
                    self._next_id = max(item.get("id", 0) for item in self._items) + 1
            except (json.JSONDecodeError, OSError):
                pass

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(
                {
                    "items": self._items,
                    "columns": self._columns,
                    "filters": self._filters,
                    "title": self._title,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    def get_all(self) -> dict[str, Any]:
        return {
            "items": list(self._items),
            "columns": self._columns,
            "filters": list(self._filters),
            "title": self._title,
        }

    def save_all(
        self,
        items: list[dict[str, Any]],
        columns: int | None = None,
        filters: list[dict[str, Any]] | None = None,
        title: str | None = None,
    ) -> dict[str, Any]:
        for item in items:
            if "id" not in item:
                item["id"] = self._next_id
                self._next_id += 1
        self._items = items
        if columns is not None:
            self._columns = columns
        if filters is not None:
            self._filters = filters
        if title is not None:
            self._title = title
        if self._items:
            self._next_id = max(item.get("id", 0) for item in self._items) + 1
        self._save()
        return self.get_all()

    def clear(self) -> None:
        self._items = []
        self._columns = 0
        self._filters = []
        self._title = ""
        self._next_id = 1
        self._save()


def _empty_dashboard() -> dict[str, Any]:
    return {"items": [], "columns": 0, "filters": [], "title": ""}


_DASHBOARD_FILTER_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DASHBOARD_FILTER_OPS = {"eq", "neq", "gt", "gte", "lt", "lte", "contains", "in"}


def _quote_dashboard_filter_identifier(identifier: str) -> str:
    """Quote a dashboard result-column filter identifier.

    Dashboard filters are applied to a wrapped query result, so we intentionally
    support only simple result column names here.
    """
    if not _DASHBOARD_FILTER_COLUMN_RE.match(identifier):
        raise ValueError(f"Invalid dashboard filter column: {identifier!r}")
    return f'"{identifier}"'


def _dashboard_filter_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _build_dashboard_filter_condition(filter_data: dict[str, Any]) -> str | None:
    if filter_data.get("enabled") is False:
        return None
    column = str(filter_data.get("column") or "").strip()
    if not column:
        return None
    op = str(filter_data.get("operator") or "eq").strip().lower()
    if op not in _DASHBOARD_FILTER_OPS:
        raise ValueError(f"Unsupported dashboard filter operator: {op}")

    quoted_column = _quote_dashboard_filter_identifier(column)
    value = filter_data.get("value")

    if op == "in":
        values = value if isinstance(value, list) else [value]
        values = [v for v in values if v is not None and str(v) != ""]
        if not values:
            return None
        return f"{quoted_column} IN ({', '.join(_dashboard_filter_literal(v) for v in values)})"
    if value is None or str(value) == "":
        return None
    if op == "contains":
        escaped = str(value).lower().replace("'", "''")
        return f"LOWER(CAST({quoted_column} AS TEXT)) LIKE '%{escaped}%'"

    sql_op = {
        "eq": "=",
        "neq": "!=",
        "gt": ">",
        "gte": ">=",
        "lt": "<",
        "lte": "<=",
    }[op]
    return f"{quoted_column} {sql_op} {_dashboard_filter_literal(value)}"


def _apply_dashboard_filters(sql: str, filters: list[dict[str, Any]]) -> str:
    conditions = [
        condition
        for item in filters
        if (condition := _build_dashboard_filter_condition(item)) is not None
    ]
    if not conditions:
        return sql
    return (
        "SELECT *\n"
        "FROM (\n"
        f"{sql}\n"
        ") AS datasight_dashboard_source\n"
        f"WHERE {' AND '.join(conditions)}"
    )


def _normalize_dashboard_filter_values(values: list[Any], limit: int) -> list[Any]:
    result: list[Any] = []
    seen: set[str] = set()
    for value in values:
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except (TypeError, ValueError):
            pass
        if hasattr(value, "item"):
            value = value.item()
        key = str(value)
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
        if len(result) >= limit:
            break
    return result


# ---------------------------------------------------------------------------
# Application State
# ---------------------------------------------------------------------------


class AppState:
    """Runtime state for the datasight web application."""

    def __init__(self) -> None:
        self.llm_client: LLMClient | None = None
        self.sql_runner: SqlRunner | None = None
        self.system_prompt: str = ""
        self.model: str = "claude-haiku-4-5-20251001"
        self.llm_provider: str = "anthropic"
        self.conversations: ConversationStore | None = None
        self.bookmarks: BookmarkStore | None = None
        self.dashboard: DashboardStore | None = None
        self.reports: ReportStore | None = None
        self.schema_info: list[dict[str, Any]] = []
        self.example_queries_list: list[dict[str, str]] = []
        self.query_logger: QueryLogger | None = None
        self.confirm_sql: bool = False
        self.explain_sql: bool = False
        self.clarify_sql: bool = True
        self.show_cost: bool = True
        self.show_provenance: bool = False
        self.schema_text: str = ""
        self.schema_map: dict[str, set[str]] = {}
        self.measure_rules: dict[tuple[str, str], Any] = {}
        self.sql_dialect: str = "duckdb"
        self.project_dir: str | None = None
        self.project_loaded: bool = False
        self.pending_confirms: dict[str, dict[str, Any]] = {}
        self._response_cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._response_cache_max = 100
        self._insight_cache: dict[str, Any] = {}
        self._max_history_pairs = 10
        self.max_cost_usd_per_turn: float | None = 1.0
        self.max_output_tokens: int = 4096
        # Per-session locks to prevent concurrent chat on the same session
        self._session_locks: dict[str, asyncio.Lock] = {}
        # Lock for state-level mutations (project load, settings changes)
        self.state_lock = asyncio.Lock()
        # Ephemeral explore session state
        self.is_ephemeral: bool = False
        self.ephemeral_tables_info: list[dict[str, Any]] = []
        self.time_series_configs: list[dict[str, Any]] = []

    def clear_project(self) -> None:
        """Clear project-specific state."""
        if self.sql_runner and hasattr(self.sql_runner, "close"):
            try:
                self.sql_runner.close()
            except Exception:
                pass
        self.sql_runner = None
        self.schema_info = []
        self.example_queries_list = []
        self.schema_text = ""
        self.schema_map = {}
        self.measure_rules = {}
        self.system_prompt = ""
        self.project_dir = None
        self.project_loaded = False
        # Reset LLM state to force reinitialization on next project load
        self.llm_client = None
        self.model = ""
        self.llm_provider = ""
        self.conversations = None
        self.bookmarks = None
        self.dashboard = None
        self.reports = None
        self.query_logger = None
        self._response_cache.clear()
        self._insight_cache.clear()
        self.time_series_configs = []
        # Reset ephemeral state
        self.is_ephemeral = False
        self.ephemeral_tables_info = []
        self._ephemeral_messages = {}
        self._session_locks.clear()

    def rebuild_system_prompt(self) -> None:
        """Rebuild the system prompt after settings change."""
        self.system_prompt = build_system_prompt(
            self.schema_text,
            mode="web",
            explain_sql=self.explain_sql,
            clarify_sql=self.clarify_sql,
            dialect=self.sql_dialect,
        )
        self._response_cache.clear()
        self._insight_cache.clear()

    def get_insight_cache(self, key: str) -> Any | None:
        """Return cached deterministic UI diagnostics."""
        return self._insight_cache.get(key)

    def put_insight_cache(self, key: str, value: Any) -> None:
        """Store cached deterministic UI diagnostics."""
        self._insight_cache[key] = value

    def clear_insight_cache(self) -> None:
        """Drop cached deterministic UI diagnostics."""
        self._insight_cache.clear()

    def clear_sql_cache(self) -> None:
        """Drop cached SQL results (if the runner is a CachingSqlRunner)."""
        if isinstance(self.sql_runner, CachingSqlRunner):
            self.sql_runner.clear_cache()

    @staticmethod
    def _cache_key(question: str) -> str:
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

    def get_session_lock(self, session_id: str) -> asyncio.Lock:
        """Get or create a lock for the given session."""
        if session_id not in self._session_locks:
            self._session_locks[session_id] = asyncio.Lock()
        return self._session_locks[session_id]

    def get_session_messages(self, session_id: str) -> list[dict[str, Any]]:
        """Get messages for a session."""
        if self.conversations is not None:
            return self.conversations.get(session_id)["messages"]
        # Ephemeral session — use in-memory message list
        if not hasattr(self, "_ephemeral_messages"):
            self._ephemeral_messages: dict[str, list[dict[str, Any]]] = {}
        return self._ephemeral_messages.setdefault(session_id, [])

    async def save_session(self, session_id: str) -> None:
        """Save session data if conversations store is available (no-op for ephemeral)."""
        if self.conversations is not None:
            await asyncio.to_thread(self.conversations.save, session_id)

    def trim_messages(
        self,
        messages: list[dict[str, Any]],
        max_history_pairs: int | None = None,
    ) -> list[dict[str, Any]]:
        """Keep only recent messages to bound input token growth."""
        limit = max_history_pairs if max_history_pairs is not None else self._max_history_pairs
        exchange_starts: list[int] = []
        for i, msg in enumerate(messages):
            if msg["role"] == "user" and isinstance(msg["content"], str):
                exchange_starts.append(i)

        if len(exchange_starts) <= limit:
            return messages

        cut = exchange_starts[-limit]
        return messages[cut:]

    def trim_messages_for_provider(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Keep provider-specific chat history within request-size limits."""
        if self.llm_provider == "github":
            return self.trim_messages(messages, max_history_pairs=4)
        return self.trim_messages(messages)


# Global state instance
_state = AppState()


def get_state() -> AppState:
    """Dependency that provides the application state."""
    return _state


# ---------------------------------------------------------------------------
# LLM Client Initialization
# ---------------------------------------------------------------------------


def init_llm_client(state: AppState) -> None:
    """Initialize or reinitialize the LLM client from environment variables."""
    settings = Settings.from_env()

    try:
        client = create_llm_client(
            provider=settings.llm.provider,
            api_key=settings.llm.api_key,
            base_url=settings.llm.base_url,
            timeout=settings.llm.timeout,
            model=settings.llm.model,
        )
        # Only update state after successful creation
        state.llm_client = client
        state.model = settings.llm.model
        state.llm_provider = settings.llm.provider
    except (LLMError, ConfigurationError) as e:
        logger.warning(f"Failed to initialize LLM client: {e}")
        # Clear client on failure to prevent stale state
        state.llm_client = None
        state.model = ""
        state.llm_provider = ""


async def _build_project_health(state: AppState) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def add_check(name: str, ok: bool, detail: str, category: str, remediation: str) -> None:
        checks.append(
            {
                "name": name,
                "ok": ok,
                "detail": detail,
                "category": category,
                "remediation": remediation if not ok else "",
            }
        )

    project_dir = Path(state.project_dir).resolve() if state.project_dir else None
    env_path = project_dir / ".env" if project_dir else None
    add_check(
        ".env",
        bool(env_path and env_path.exists()),
        str(env_path) if env_path else "No project loaded",
        "project",
        "Create or load a project directory with a .env file.",
    )

    try:
        settings = Settings.from_env(
            str(env_path) if env_path and env_path.exists() else None,
            override=False,
        )
        validation_errors = settings.validate()
        add_check(
            "LLM settings",
            not validation_errors,
            "; ".join(validation_errors) if validation_errors else settings.llm.provider,
            "config",
            "Fix missing or invalid LLM environment variables in .env.",
        )
    except Exception as exc:
        settings = None
        add_check("LLM settings", False, str(exc), "config", "Fix the LLM configuration in .env.")

    if project_dir:
        for name in ("schema_description.md", "queries.yaml"):
            path = project_dir / name
            add_check(
                name,
                path.exists(),
                str(path),
                "project",
                f"Add {name} to the project root or regenerate project scaffolding.",
            )

        datasight_dir = project_dir / ".datasight"
        try:
            datasight_dir.mkdir(parents=True, exist_ok=True)
            probe = datasight_dir / ".health-write-test"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
            add_check(".datasight writable", True, str(datasight_dir), "project", "")
        except OSError as exc:
            add_check(
                ".datasight writable",
                False,
                f"{datasight_dir}: {exc}",
                "project",
                "Fix permissions on the project directory so datasight can write local state.",
            )
    else:
        add_check(
            "schema_description.md",
            False,
            "No project loaded",
            "project",
            "Load a project first, then add schema_description.md if you want richer context.",
        )
        add_check(
            "queries.yaml",
            False,
            "No project loaded",
            "project",
            "Load a project first, then add queries.yaml for example queries.",
        )
        add_check(
            ".datasight writable",
            False,
            "No project loaded",
            "project",
            "Load a project first so datasight can create its local state directory.",
        )

    if settings is not None:
        db_detail = settings.database.mode
        db_ok = True
        if settings.database.mode in ("duckdb", "sqlite"):
            db_path = settings.database.path
            if db_path and not os.path.isabs(db_path) and project_dir:
                db_path = str(project_dir / db_path)
            db_ok = bool(db_path) and os.path.exists(db_path)
            db_detail = str(db_path)
        elif settings.database.mode == "postgres":
            db_ok = bool(
                settings.database.postgres_url
                or (
                    settings.database.postgres_database
                    and settings.database.postgres_user
                    and settings.database.postgres_host
                )
            )
            db_detail = settings.database.postgres_url or (
                f"{settings.database.postgres_user}@{settings.database.postgres_host}:"
                f"{settings.database.postgres_port}/{settings.database.postgres_database}"
            )
        elif settings.database.mode == "flightsql":
            db_ok = bool(settings.database.flight_uri)
            db_detail = settings.database.flight_uri
        elif settings.database.mode == "spark":
            db_ok = bool(settings.database.spark_remote)
            db_detail = settings.database.spark_remote
        add_check(
            "Database config",
            db_ok,
            db_detail or settings.database.mode,
            "config",
            "Fix the database connection settings in .env.",
        )
    else:
        add_check(
            "Database config",
            False,
            "Settings unavailable",
            "config",
            "Load valid settings from .env.",
        )

    if state.sql_runner is not None:
        try:
            await state.sql_runner.run_sql("SELECT 1 AS ok")
            add_check("Database connectivity", True, "SELECT 1", "connectivity", "")
        except Exception as exc:
            add_check(
                "Database connectivity",
                False,
                str(exc),
                "connectivity",
                "Verify the database is reachable and the configured credentials are valid.",
            )
    else:
        add_check(
            "Database connectivity",
            False,
            "No database connection",
            "connectivity",
            "Load a project or fix the database config so datasight can open a connection.",
        )

    config_failures = sum(
        1 for check in checks if not check["ok"] and check["category"] == "config"
    )
    connectivity_failures = sum(
        1 for check in checks if not check["ok"] and check["category"] == "connectivity"
    )
    project_failures = sum(
        1 for check in checks if not check["ok"] and check["category"] == "project"
    )

    # Surface the configured backend identity at the top level so the UI
    # can render compact "Project / DB / LLM" rows without parsing strings
    # out of individual check details.
    db_mode = settings.database.mode if settings is not None else None
    db_target = next(
        (c["detail"] for c in checks if c["name"] == "Database config" and c["ok"]),
        None,
    )
    llm_provider = settings.llm.provider if settings is not None else None

    return {
        "project_loaded": state.project_loaded,
        "project_dir": state.project_dir,
        "db_mode": db_mode,
        "db_target": db_target,
        "llm_provider": llm_provider,
        "checks": checks,
        "summary": {
            "ok_count": sum(1 for check in checks if check["ok"]),
            "fail_count": sum(1 for check in checks if not check["ok"]),
            "config_failures": config_failures,
            "connectivity_failures": connectivity_failures,
            "project_failures": project_failures,
        },
    }


async def _get_cached_insight(
    state: AppState,
    cache_key: str,
    builder,
) -> tuple[Any, bool]:
    """Return cached deterministic UI data or compute it once."""
    cached = state.get_insight_cache(cache_key)
    if cached is not None:
        logger.debug(f"[insight-cache] HIT {cache_key}")
        return cached, True

    value = await builder()
    state.put_insight_cache(cache_key, value)
    logger.debug(f"[insight-cache] STORED {cache_key}")
    return value, False


# ---------------------------------------------------------------------------
# Project Loading
# ---------------------------------------------------------------------------


async def load_project(project_dir: str, state: AppState) -> dict[str, Any]:
    """Load a project directory, initializing DB connection and schema.

    Parameters
    ----------
    project_dir:
        Path to the project directory.
    state:
        Application state instance.

    Returns
    -------
    Project info dict on success.

    Raises
    ------
    ProjectError:
        If project validation or loading fails.
    """
    project_dir = str(Path(project_dir).resolve())

    is_valid, error = validate_project_dir(project_dir)
    if not is_valid:
        raise ProjectError(error)

    state.clear_project()

    # Restore original shell env vars to prevent leaking settings between projects
    restore_original_env()

    # Load settings from project .env (override=True so project config wins
    # over any shell/baseline env vars that were restored above)
    env_path = os.path.join(project_dir, ".env")
    settings = Settings.from_env(env_path if os.path.exists(env_path) else None, override=True)

    # Reinitialize LLM client
    init_llm_client(state)
    if state.llm_client is None:
        raise ProjectError("Failed to initialize LLM client. Check API key and provider settings.")

    # Validate database file exists for file-based databases
    db_path = settings.database.path
    if settings.database.mode in ("duckdb", "sqlite"):
        if not os.path.isabs(db_path):
            db_path = str(Path(project_dir) / db_path)
        if not os.path.exists(db_path):
            raise ProjectError(f"Database file not found: {db_path}")

    state.sql_dialect = settings.database.sql_dialect

    try:
        state.sql_runner = create_sql_runner_from_settings(
            settings.database,
            project_dir,
            sql_cache_max_bytes=settings.app.sql_cache_max_bytes,
        )
    except Exception as e:
        logger.exception("Failed to create SQL runner")
        raise ProjectError(f"Failed to connect to database: {e}") from e

    state.project_dir = project_dir
    add_recent_project(project_dir)

    # Set up project-specific storage
    datasight_dir = Path(project_dir) / ".datasight"
    state.conversations = ConversationStore(datasight_dir / "conversations")
    state.bookmarks = BookmarkStore(datasight_dir / "bookmarks.json")
    state.dashboard = DashboardStore(datasight_dir / "dashboard.json")
    state.reports = ReportStore(datasight_dir / "reports.json")

    # Load settings
    state.confirm_sql = settings.app.confirm_sql
    state.explain_sql = settings.app.explain_sql
    state.clarify_sql = settings.app.clarify_sql
    state.show_provenance = settings.app.show_provenance
    state._max_history_pairs = settings.app.max_history_pairs
    state._response_cache_max = settings.app.response_cache_max
    state.max_cost_usd_per_turn = settings.app.max_cost_usd_per_turn
    state.max_output_tokens = settings.app.max_output_tokens

    log_path = os.environ.get(
        "QUERY_LOG_PATH",
        os.path.join(project_dir, ".datasight", "query_log.jsonl"),
    )
    state.query_logger = QueryLogger(path=log_path)

    # Load schema and introspect database
    user_desc = load_schema_description(os.environ.get("SCHEMA_DESCRIPTION_PATH"), project_dir)
    user_desc = await resolve_schema_description_links(user_desc)

    schema_config = load_schema_config(None, project_dir)
    allowed_tables: set[str] | None = None
    if schema_config is not None:
        allowed_tables = {
            e["name"] for e in schema_config.get("tables", []) if e.get("name")
        } or None
    tables = await introspect_schema(
        state.sql_runner.run_sql,
        runner=state.sql_runner,
        allowed_tables=allowed_tables,
    )
    if schema_config is not None:
        tables = filter_tables(tables, schema_config)
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

    from datasight.identifiers import configure_runner_identifier_quoting

    configure_runner_identifier_quoting(state.sql_runner, state.schema_info)

    state.schema_text = format_schema_context(tables, user_desc)
    state.schema_map = build_schema_map(state.schema_info)
    measure_overrides = load_measure_overrides(None, project_dir)
    state.measure_rules = build_measure_rule_map(measure_overrides)

    measure_text = format_measure_prompt_context(
        await build_measure_overview(
            state.schema_info, state.sql_runner.run_sql, measure_overrides
        )
    )
    if measure_text:
        state.schema_text += measure_text

    time_series_configs = load_time_series_config(None, project_dir)
    state.time_series_configs = time_series_configs
    ts_text = format_time_series_prompt_context(time_series_configs)
    if ts_text:
        state.schema_text += ts_text

    example_queries = load_example_queries(os.environ.get("EXAMPLE_QUERIES_PATH"), project_dir)
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
        "has_time_series": bool(state.time_series_configs),
    }


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------


async def _startup() -> None:
    """Initialize the LLM client on startup."""
    from dotenv import load_dotenv

    load_dotenv()
    load_global_env(override=False)

    # Capture env after root + global .env are loaded as baseline for project
    # switching. Global API keys persist across switches; project values are
    # dropped when restore_original_env() runs.
    capture_original_env()

    init_llm_client(_state)

    # Check for auto-load project
    auto_load_project = os.environ.get("DATASIGHT_AUTO_LOAD_PROJECT")
    if auto_load_project:
        try:
            await load_project(auto_load_project, _state)
            logger.info(f"Auto-loaded project: {auto_load_project}")
        except ProjectError as e:
            logger.error(f"Failed to auto-load project {auto_load_project}: {e}")

    socket_path = os.environ.get("DATASIGHT_UNIX_SOCKET", "").strip()
    port = os.environ.get("PORT", "8084")
    if _state.project_loaded:
        logger.info(f"datasight ready (model={_state.model}, project={_state.project_dir})")
    else:
        logger.info(f"datasight ready (model={_state.model}, no project loaded)")
    if socket_path:
        print(f"\n  Ready — listening on UNIX socket {socket_path}\n")
    else:
        print(f"\n  Ready — open http://localhost:{port} in your browser\n")


# ---------------------------------------------------------------------------
# Tool Execution Helper
# ---------------------------------------------------------------------------


async def execute_tool_web(
    name: str,
    input_data: dict[str, Any],
    state: AppState,
    session_id: str = "",
    user_question: str = "",
    turn_id: str = "",
) -> tuple[str, str | None, str | None, dict[str, Any], dict[str, Any] | None]:
    """Execute a tool call via the shared agent module.

    Returns (result_text_for_llm, optional_html_for_ui, optional_chart_html, meta, plotly_spec).
    """
    if state.sql_runner is None:
        raise ConfigurationError("SQL runner not initialized")

    result = await execute_tool(
        name,
        input_data,
        run_sql=state.sql_runner.run_sql,
        schema_map=state.schema_map or None,
        dialect=state.sql_dialect,
        measure_rules=state.measure_rules or None,
        query_logger=state.query_logger,
        session_id=session_id,
        user_question=user_question,
        turn_id=turn_id,
    )
    return result.result_text, result.result_html, None, result.meta, result.plotly_spec


def _build_tool_provenance(
    *,
    turn_id: str,
    question: str,
    model: str,
    dialect: str,
    project_dir: str,
    tool_call_id: str,
    meta: dict[str, Any],
) -> dict[str, Any]:
    """Build answer provenance from tool metadata."""
    status = "error" if meta.get("error") else "success"
    return {
        "turn_id": turn_id,
        "question": question,
        "model": model,
        "dialect": dialect,
        "project_dir": project_dir,
        "tool_call_id": tool_call_id,
        "tool": meta.get("tool"),
        "sql": meta.get("sql"),
        "formatted_sql": meta.get("formatted_sql"),
        "validation": meta.get("validation", {"status": "not_run", "errors": []}),
        "execution": {
            "status": status,
            "execution_time_ms": meta.get("execution_time_ms"),
            "row_count": meta.get("row_count"),
            "column_count": meta.get("column_count"),
            "columns": meta.get("columns", []),
            "error": meta.get("error"),
            "timestamp": meta.get("timestamp"),
        },
    }


def _build_turn_provenance(
    *,
    turn_id: str,
    question: str,
    model: str,
    dialect: str,
    project_dir: str,
    tools: list[dict[str, Any]],
    cost_data: dict[str, Any],
    api_calls: int,
    input_tokens: int,
    output_tokens: int,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
) -> dict[str, Any]:
    """Build provenance payload for a completed assistant turn."""
    warnings = [
        f"{tool.get('tool') or 'tool'} failed: {tool['execution']['error']}"
        for tool in tools
        if tool.get("execution", {}).get("error")
    ]
    return {
        "turn_id": turn_id,
        "question": question,
        "model": model,
        "dialect": dialect,
        "project_dir": project_dir,
        "tools": tools,
        "llm": {
            "api_calls": api_calls,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cache_creation_input_tokens": cache_creation_input_tokens,
            "cache_read_input_tokens": cache_read_input_tokens,
            "estimated_cost": cost_data.get("estimated_cost"),
        },
        "warnings": warnings,
    }


def _compact_chart_tool_result_for_stream(
    data: dict[str, Any],
    *,
    session_id: str,
    event_index: int,
    can_fetch_spec: bool,
) -> dict[str, Any]:
    """Replace large resolved Plotly specs with a fetchable reference for SSE."""
    if not can_fetch_spec:
        return data
    if data.get("type") != "chart":
        return data
    if not (data.get("plotly_spec") or data.get("plotlySpec")):
        return data
    return {
        "html": "",
        "type": "chart",
        "title": data.get("title"),
        "plotly_spec_ref": {
            "session_id": session_id,
            "event_index": event_index,
        },
    }


# ---------------------------------------------------------------------------
# Chat Generator
# ---------------------------------------------------------------------------


def _truncate_session_at_turn(
    messages: list[dict[str, Any]],
    evt_log: list[dict[str, Any]],
    turn_index: int,
) -> None:
    """Drop turn ``turn_index`` and everything after it from a session.

    A turn is anchored by a user prompt: the Nth ``USER_MESSAGE`` event in
    ``evt_log`` and the Nth ``role=user`` string entry in ``messages``. After
    truncation, the next ``messages.append({"role": "user", ...})`` and matching
    user-message event resume the conversation cleanly at turn ``turn_index``.
    Out-of-range indices leave the lists untouched.
    """
    if turn_index < 0:
        return

    user_count = 0
    evt_cut = len(evt_log)
    for i, evt in enumerate(evt_log):
        if evt.get("event") == EventType.USER_MESSAGE:
            if user_count == turn_index:
                evt_cut = i
                break
            user_count += 1
    del evt_log[evt_cut:]

    user_count = 0
    msg_cut = len(messages)
    for i, msg in enumerate(messages):
        if msg.get("role") == "user" and isinstance(msg.get("content"), str):
            if user_count == turn_index:
                msg_cut = i
                break
            user_count += 1
    del messages[msg_cut:]


async def generate_chat_response(
    message: str,
    session_id: str,
    state: AppState,
    request: Request | None = None,
    truncate_before_turn: int | None = None,
) -> AsyncIterator[str]:
    """Generate SSE events for a chat message."""
    if state.llm_client is None:
        raise ConfigurationError("LLM not configured. Open Settings to add your API key.")

    messages = state.get_session_messages(session_id)
    conv = state.conversations.get(session_id) if state.conversations else None
    evt_log_pre = conv["events"] if conv else []
    if truncate_before_turn is not None:
        _truncate_session_at_turn(messages, evt_log_pre, truncate_before_turn)
        if conv and truncate_before_turn == 0:
            conv["title"] = "Untitled"
    messages.append({"role": "user", "content": message})
    turn_id = str(uuid.uuid4())
    tool_provenance: list[dict[str, Any]] = []

    evt_log = evt_log_pre
    evt_log.append(
        {"event": EventType.USER_MESSAGE, "data": {"text": message, "turn_id": turn_id}}
    )

    if conv and conv["title"] == "Untitled":
        conv["title"] = message[:80] + ("..." if len(message) > 80 else "")
    await state.save_session(session_id)

    is_first_turn = len(messages) == 1
    max_iterations = 15

    # Check cache for first-turn questions
    if is_first_turn:
        cached = state.cache_get(message)
        if cached is not None:
            logger.info(f"[cache] HIT for question: {message[:60]}")
            for evt in cached["events"]:
                evt_log.append(evt)
                event_data = evt["data"]
                if evt["event"] == EventType.TOOL_RESULT:
                    event_index = len(evt_log) - 1
                    event_data = _compact_chart_tool_result_for_stream(
                        event_data,
                        session_id=session_id,
                        event_index=event_index,
                        can_fetch_spec=state.conversations is not None,
                    )
                    if event_data is not evt["data"]:
                        await state.save_session(session_id)
                yield f"event: {evt['event']}\ndata: {json.dumps(event_data)}\n\n"
            for msg in cached["messages"]:
                messages.append(msg)
            await state.save_session(session_id)
            cached_cost = cached.get("cost", {})
            yield f"event: done\ndata: {json.dumps(cached_cost)}\n\n"
            if cached.get("suggestions"):
                evt_log.append(
                    {
                        "event": EventType.SUGGESTIONS,
                        "data": {"suggestions": cached["suggestions"]},
                    }
                )
                await state.save_session(session_id)
                yield f"event: {EventType.SUGGESTIONS}\ndata: {json.dumps({'suggestions': cached['suggestions']})}\n\n"
            return

    _evt_start = len(evt_log)
    _msg_start = len(messages)

    total_input_tokens = 0
    total_output_tokens = 0
    total_cache_creation_input_tokens = 0
    total_cache_read_input_tokens = 0
    api_calls = 0

    for _ in range(max_iterations):
        # Stop early if the client disconnected (e.g. user clicked Stop)
        if request is not None and await request.is_disconnected():
            logger.info(
                f"[chat] Client disconnected, stopping generation for session {session_id}"
            )
            return

        trimmed = state.trim_messages_for_provider(messages)
        try:
            response = await state.llm_client.create_message(
                model=state.model,
                max_tokens=state.max_output_tokens,
                system=state.system_prompt,
                tools=WEB_TOOLS,
                messages=trimmed,
            )
        except LLMError as e:
            logger.error(f"LLM API error: {e}")
            evt_log.append(
                {
                    "event": EventType.ASSISTANT_MESSAGE,
                    "data": {"text": f"Error: {e}"},
                }
            )
            await state.save_session(session_id)
            yield f"event: token\ndata: {json.dumps({'text': f'Error: {e}'})}\n\n"
            yield "event: done\ndata: {}\n\n"
            return
        except Exception as e:
            logger.exception("Unexpected LLM error")
            evt_log.append(
                {
                    "event": EventType.ASSISTANT_MESSAGE,
                    "data": {"text": f"Error: {e}"},
                }
            )
            await state.save_session(session_id)
            yield f"event: token\ndata: {json.dumps({'text': f'Error: {e}'})}\n\n"
            yield "event: done\ndata: {}\n\n"
            return

        api_calls += 1
        total_input_tokens += response.usage.input_tokens
        total_output_tokens += response.usage.output_tokens
        total_cache_creation_input_tokens += response.usage.cache_creation_input_tokens
        total_cache_read_input_tokens += response.usage.cache_read_input_tokens
        logger.info(
            f"[tokens] call={api_calls} "
            f"input={response.usage.input_tokens} output={response.usage.output_tokens} "
            f"cache_create={response.usage.cache_creation_input_tokens} "
            f"cache_read={response.usage.cache_read_input_tokens}"
        )

        if state.max_cost_usd_per_turn is not None:
            running_cost = build_cost_data(
                state.model,
                api_calls,
                total_input_tokens,
                total_output_tokens,
                cache_creation_input_tokens=total_cache_creation_input_tokens,
                cache_read_input_tokens=total_cache_read_input_tokens,
                provider=state.llm_provider,
            )["estimated_cost"]
            if running_cost is not None and running_cost > state.max_cost_usd_per_turn:
                logger.warning(
                    f"[chat] Cost budget exceeded: ${running_cost:.4f} > "
                    f"${state.max_cost_usd_per_turn:.2f} (api_calls={api_calls})"
                )
                budget_text = (
                    f"Stopped: estimated cost ${running_cost:.2f} exceeded the "
                    f"${state.max_cost_usd_per_turn:.2f} budget for this question. "
                    "Try a more specific question or raise the budget."
                )
                evt_log.append(
                    {"event": EventType.ASSISTANT_MESSAGE, "data": {"text": budget_text}}
                )
                await state.save_session(session_id)
                yield f"event: token\ndata: {json.dumps({'text': budget_text})}\n\n"
                yield "event: done\ndata: {}\n\n"
                return

        if response.stop_reason == "max_tokens":
            logger.warning(
                f"[chat] LLM response truncated at max_tokens={state.max_output_tokens} "
                f"(model={state.model}, api_calls={api_calls})"
            )
            partial = "".join(b.text for b in response.content if isinstance(b, TextBlock))
            notice = (
                f"\n\n_Response truncated: model hit the {state.max_output_tokens}-token "
                "output limit. Try a narrower question or raise MAX_OUTPUT_TOKENS._"
            )
            final_text = (partial + notice) if partial else notice.lstrip()
            messages.append({"role": "assistant", "content": final_text})
            evt_log.append({"event": EventType.ASSISTANT_MESSAGE, "data": {"text": final_text}})
            await state.save_session(session_id)
            yield f"event: token\ndata: {json.dumps({'text': final_text})}\n\n"
            yield "event: done\ndata: {}\n\n"
            return

        if response.stop_reason == "tool_use":
            messages.append(
                {
                    "role": "assistant",
                    "content": serialize_content(response.content),
                }
            )

            # Stream text blocks before tool execution
            for block in response.content:
                if isinstance(block, TextBlock) and block.text.strip():
                    for word_i, word in enumerate(block.text.split(" ")):
                        chunk = word if word_i == 0 else " " + word
                        yield f"event: token\ndata: {json.dumps({'text': chunk})}\n\n"
                    evt_log.append(
                        {
                            "event": EventType.ASSISTANT_MESSAGE,
                            "data": {"text": block.text},
                        }
                    )
                    yield "event: explanation_done\ndata: {}\n\n"

            tool_results = []
            tool_blocks = [b for b in response.content if isinstance(b, ToolUseBlock)]
            disconnected = False
            for block in tool_blocks:
                # Check for client disconnect before running each tool
                if not disconnected and request is not None and await request.is_disconnected():
                    logger.info("[chat] Client disconnected before tool execution, stopping")
                    disconnected = True

                if disconnected:
                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": "Request cancelled by user.",
                        }
                    )
                    continue

                tool_input = dict(block.input)

                # SQL confirmation flow
                if state.confirm_sql and block.name in ("run_sql", "visualize_data"):
                    async for evt in _handle_sql_confirmation(block, tool_input, state):
                        if evt.startswith("SKIP:"):
                            tool_results.append(
                                {
                                    "type": "tool_result",
                                    "tool_use_id": block.id,
                                    "content": evt[5:],
                                }
                            )
                            break
                        yield evt
                    else:
                        # Confirmation approved, continue with execution
                        pass
                    if tool_results and tool_results[-1]["tool_use_id"] == block.id:
                        continue

                tool_start_data = {"tool": block.name, "input": tool_input}
                evt_log.append({"event": EventType.TOOL_START, "data": tool_start_data})
                yield f"event: {EventType.TOOL_START}\ndata: {json.dumps(tool_start_data)}\n\n"

                result_text, result_html, _, meta, plotly_spec = await execute_tool_web(
                    block.name,
                    tool_input,
                    state,
                    session_id=session_id,
                    user_question=message,
                    turn_id=turn_id,
                )

                if result_html:
                    is_chart = block.name == "visualize_data" and plotly_spec is not None
                    result_title = tool_input.get("title", message) if is_chart else message
                    tr_data: dict[str, Any] = {
                        # New chart results render from plotly_spec in the frontend. Do not stream
                        # the full iframe HTML as well; large srcdoc payloads were the source of
                        # intermittent blank charts during live streaming.
                        "html": "" if is_chart else result_html,
                        "type": "chart" if is_chart else "table",
                        "title": result_title,
                    }
                    if is_chart:
                        tr_data["plotly_spec"] = plotly_spec
                    evt_log.append({"event": EventType.TOOL_RESULT, "data": tr_data})
                    streamed_tr_data = _compact_chart_tool_result_for_stream(
                        tr_data,
                        session_id=session_id,
                        event_index=len(evt_log) - 1,
                        can_fetch_spec=state.conversations is not None,
                    )
                    if streamed_tr_data is not tr_data:
                        await state.save_session(session_id)
                    yield (
                        f"event: {EventType.TOOL_RESULT}\ndata: {json.dumps(streamed_tr_data)}\n\n"
                    )

                if meta:
                    evt_log.append({"event": EventType.TOOL_DONE, "data": meta})
                    yield f"event: {EventType.TOOL_DONE}\ndata: {json.dumps(meta)}\n\n"
                    if block.name in ("run_sql", "visualize_data"):
                        tool_provenance.append(
                            _build_tool_provenance(
                                turn_id=turn_id,
                                question=message,
                                model=state.model,
                                dialect=state.sql_dialect,
                                project_dir=state.project_dir or "",
                                tool_call_id=block.id,
                                meta=meta,
                            )
                        )
                    await state.save_session(session_id)

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
        suggestions = extract_suggestions(text)
        if suggestions:
            text = re.split(r"\n---\s*\n", text, maxsplit=1)[0].rstrip()

        messages.append({"role": "assistant", "content": text})
        evt_log.append({"event": EventType.ASSISTANT_MESSAGE, "data": {"text": text}})
        await state.save_session(session_id)

        for i, word in enumerate(text.split(" ")):
            chunk = word if i == 0 else " " + word
            yield f"event: token\ndata: {json.dumps({'text': chunk})}\n\n"
            await asyncio.sleep(0.015)

        log_query_cost(
            state.model,
            api_calls,
            total_input_tokens,
            total_output_tokens,
            cache_creation_input_tokens=total_cache_creation_input_tokens,
            cache_read_input_tokens=total_cache_read_input_tokens,
            provider=state.llm_provider,
        )
        cost_data = build_cost_data(
            state.model,
            api_calls,
            total_input_tokens,
            total_output_tokens,
            cache_creation_input_tokens=total_cache_creation_input_tokens,
            cache_read_input_tokens=total_cache_read_input_tokens,
            provider=state.llm_provider,
        )
        if state.query_logger:
            state.query_logger.log_cost(
                session_id=session_id,
                user_question=message,
                api_calls=api_calls,
                input_tokens=total_input_tokens,
                output_tokens=total_output_tokens,
                cache_creation_input_tokens=total_cache_creation_input_tokens,
                cache_read_input_tokens=total_cache_read_input_tokens,
                estimated_cost=cost_data.get("estimated_cost"),
                turn_id=turn_id,
            )

        provenance = _build_turn_provenance(
            turn_id=turn_id,
            question=message,
            model=state.model,
            dialect=state.sql_dialect,
            project_dir=state.project_dir or "",
            tools=tool_provenance,
            cost_data=cost_data,
            api_calls=api_calls,
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens,
            cache_creation_input_tokens=total_cache_creation_input_tokens,
            cache_read_input_tokens=total_cache_read_input_tokens,
        )
        evt_log.append({"event": EventType.PROVENANCE, "data": provenance})
        await state.save_session(session_id)
        yield f"event: {EventType.PROVENANCE}\ndata: {json.dumps(provenance)}\n\n"

        # Cache first-turn responses
        if is_first_turn:
            new_events = evt_log[_evt_start:]
            new_messages = messages[_msg_start:]
            state.cache_put(
                message,
                {
                    "events": list(new_events),
                    "messages": list(new_messages),
                    "suggestions": suggestions,
                    "cost": cost_data,
                },
            )
            logger.info(f"[cache] STORED for question: {message[:60]}")

        yield f"event: done\ndata: {json.dumps(cost_data)}\n\n"

        if suggestions:
            evt_log.append(
                {
                    "event": EventType.SUGGESTIONS,
                    "data": {"suggestions": suggestions},
                }
            )
            await state.save_session(session_id)
            yield f"event: {EventType.SUGGESTIONS}\ndata: {json.dumps({'suggestions': suggestions})}\n\n"
        return

    log_query_cost(
        state.model,
        api_calls,
        total_input_tokens,
        total_output_tokens,
        cache_creation_input_tokens=total_cache_creation_input_tokens,
        cache_read_input_tokens=total_cache_read_input_tokens,
        provider=state.llm_provider,
    )
    cost_data = build_cost_data(
        state.model,
        api_calls,
        total_input_tokens,
        total_output_tokens,
        cache_creation_input_tokens=total_cache_creation_input_tokens,
        cache_read_input_tokens=total_cache_read_input_tokens,
        provider=state.llm_provider,
    )
    if state.query_logger:
        state.query_logger.log_cost(
            session_id=session_id,
            user_question=message,
            api_calls=api_calls,
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens,
            cache_creation_input_tokens=total_cache_creation_input_tokens,
            cache_read_input_tokens=total_cache_read_input_tokens,
            estimated_cost=cost_data.get("estimated_cost"),
            turn_id=turn_id,
        )
    provenance = _build_turn_provenance(
        turn_id=turn_id,
        question=message,
        model=state.model,
        dialect=state.sql_dialect,
        project_dir=state.project_dir or "",
        tools=tool_provenance,
        cost_data=cost_data,
        api_calls=api_calls,
        input_tokens=total_input_tokens,
        output_tokens=total_output_tokens,
        cache_creation_input_tokens=total_cache_creation_input_tokens,
        cache_read_input_tokens=total_cache_read_input_tokens,
    )
    evt_log.append({"event": EventType.PROVENANCE, "data": provenance})
    await state.save_session(session_id)
    yield f"event: {EventType.PROVENANCE}\ndata: {json.dumps(provenance)}\n\n"
    max_iter_text = "Reached maximum number of tool calls. Please try a simpler question."
    evt_log.append({"event": EventType.ASSISTANT_MESSAGE, "data": {"text": max_iter_text}})
    await state.save_session(session_id)
    yield f"event: token\ndata: {json.dumps({'text': max_iter_text})}\n\n"
    yield f"event: done\ndata: {json.dumps(cost_data)}\n\n"


async def _handle_sql_confirmation(
    block: ToolUseBlock,
    tool_input: dict[str, Any],
    state: AppState,
) -> AsyncIterator[str]:
    """Handle SQL confirmation flow.

    Yields SSE events for confirmation UI. If confirmation is rejected or times out,
    yields a "SKIP:..." event with the tool result content.
    """
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
    yield f"event: {EventType.CONFIRM_SQL}\ndata: {json.dumps(confirm_data)}\n\n"

    try:
        await asyncio.wait_for(confirm_event.wait(), timeout=300)
    except asyncio.TimeoutError:
        state.pending_confirms.pop(request_id, None)
        yield "SKIP:SQL confirmation timed out."
        return

    decision = state.pending_confirms.pop(request_id, {})
    action = decision.get("action", "reject")

    if action == "reject":
        yield "event: sql_rejected\ndata: {}\n\n"
        yield "SKIP:User rejected this SQL query. Ask what they'd like changed."
    elif action == "edit" and decision.get("sql"):
        tool_input["sql"] = decision["sql"]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse(_INDEX_HTML, media_type="text/html")


@app.get("/api/schema")
async def get_schema(state: AppState = Depends(get_state)):
    """Return discovered database schema for the sidebar."""
    return {"tables": state.schema_info}


@app.get("/api/dataset-overview")
async def get_dataset_overview(table: str | None = None, state: AppState = Depends(get_state)):
    """Return a deterministic overview of the loaded dataset."""
    if not state.project_loaded or not state.schema_info or state.sql_runner is None:
        return {"error": "No dataset loaded"}

    sql_runner = state.sql_runner
    schema_info = state.schema_info
    cache_key = "dataset-overview"
    if table:
        table_info = find_table_info(state.schema_info, table)
        if table_info is None:
            return {"error": f"Table not found: {table}"}
        schema_info = [table_info]
        cache_key = f"dataset-overview:{table_info['name'].lower()}"

    overview, cached = await _get_cached_insight(
        state,
        cache_key,
        lambda: build_dataset_overview(schema_info, sql_runner.run_sql),
    )
    return {"overview": overview, "cached": cached}


@app.get("/api/measure-overview")
async def get_measure_overview(table: str | None = None, state: AppState = Depends(get_state)):
    """Return a deterministic view of likely measures and aggregations."""
    if not state.project_loaded or state.sql_runner is None:
        return {"error": "No dataset loaded"}

    sql_runner = state.sql_runner
    schema_info = state.schema_info
    cache_key = "measure-overview"
    if table:
        table_info = find_table_info(state.schema_info, table)
        if table_info is None:
            return {"error": f"Table not found: {table}"}
        schema_info = [table_info]
        cache_key = f"measure-overview:{table_info['name'].lower()}"

    overview, cached = await _get_cached_insight(
        state,
        cache_key,
        lambda: build_measure_overview(
            schema_info,
            sql_runner.run_sql,
            load_measure_overrides(None, state.project_dir or ""),
        ),
    )
    return {"overview": overview, "cached": cached}


@app.get("/api/dimension-overview")
async def get_dimension_overview(table: str | None = None, state: AppState = Depends(get_state)):
    """Return a deterministic view of likely grouping dimensions."""
    if not state.project_loaded or state.sql_runner is None:
        return {"error": "No dataset loaded"}

    sql_runner = state.sql_runner
    schema_info = state.schema_info
    cache_key = "dimension-overview"
    if table:
        table_info = find_table_info(state.schema_info, table)
        if table_info is None:
            return {"error": f"Table not found: {table}"}
        schema_info = [table_info]
        cache_key = f"dimension-overview:{table_info['name'].lower()}"

    overview, cached = await _get_cached_insight(
        state,
        cache_key,
        lambda: build_dimension_overview(schema_info, sql_runner.run_sql),
    )
    return {"overview": overview, "cached": cached}


@app.get("/api/quality-overview")
async def get_quality_overview(table: str | None = None, state: AppState = Depends(get_state)):
    """Return a deterministic view of basic data quality signals."""
    if not state.project_loaded or state.sql_runner is None:
        return {"error": "No dataset loaded"}

    sql_runner = state.sql_runner
    schema_info = state.schema_info
    cache_key = "quality-overview"
    if table:
        table_info = find_table_info(state.schema_info, table)
        if table_info is None:
            return {"error": f"Table not found: {table}"}
        schema_info = [table_info]
        cache_key = f"quality-overview:{table_info['name'].lower()}"

    overview, cached = await _get_cached_insight(
        state,
        cache_key,
        lambda: build_quality_overview(schema_info, sql_runner.run_sql),
    )
    return {"overview": overview, "cached": cached}


@app.get("/api/timeseries-overview")
async def get_timeseries_overview(table: str | None = None, state: AppState = Depends(get_state)):
    """Return temporal completeness checks for declared time series."""
    if not state.project_loaded or state.sql_runner is None:
        return {"error": "No dataset loaded"}

    sql_runner = state.sql_runner
    configs = state.time_series_configs
    if not configs:
        return {
            "overview": {
                "configs": [],
                "summaries": [],
                "issues": [],
                "notes": ["No time_series.yaml found in the project directory."],
            },
            "cached": False,
        }

    if table:
        configs = [c for c in configs if c["table"].lower() == table.lower()]

    cache_key = f"timeseries-overview:{table.lower() if table else 'all'}"

    overview, cached = await _get_cached_insight(
        state,
        cache_key,
        lambda: build_time_series_quality(configs, sql_runner.run_sql),
    )
    overview["configs"] = configs
    return {"overview": overview, "cached": cached}


@app.get("/api/trend-overview")
async def get_trend_overview(table: str | None = None, state: AppState = Depends(get_state)):
    """Return a deterministic view of likely time-series analyses."""
    if not state.project_loaded or state.sql_runner is None:
        return {"error": "No dataset loaded"}

    sql_runner = state.sql_runner
    schema_info = state.schema_info
    cache_key = "trend-overview"
    if table:
        table_info = find_table_info(state.schema_info, table)
        if table_info is None:
            return {"error": f"Table not found: {table}"}
        schema_info = [table_info]
        cache_key = f"trend-overview:{table_info['name'].lower()}"

    overview, cached = await _get_cached_insight(
        state,
        cache_key,
        lambda: build_trend_overview(
            schema_info,
            sql_runner.run_sql,
            load_measure_overrides(None, state.project_dir or ""),
        ),
    )
    return {"overview": overview, "cached": cached}


@app.get("/api/queries")
async def get_queries(state: AppState = Depends(get_state)):
    """Return example queries."""
    return {"queries": state.example_queries_list}


@app.get("/api/recipes")
async def get_recipes(state: AppState = Depends(get_state)):
    """Return reusable prompt recipes derived from the loaded schema."""
    if not state.project_loaded or state.sql_runner is None:
        return {"recipes": [], "error": "No dataset loaded"}

    sql_runner = state.sql_runner
    recipes, cached = await _get_cached_insight(
        state,
        "prompt-recipes",
        lambda: build_prompt_recipes(
            state.schema_info,
            sql_runner.run_sql,
            load_measure_overrides(None, state.project_dir or ""),
        ),
    )
    return {"recipes": recipes, "cached": cached}


@app.get("/api/summarize")
async def summarize_dataset(state: AppState = Depends(get_state)):
    """Generate an LLM-powered summary of the dataset."""
    if not state.project_loaded or not state.schema_info:
        return StreamingResponse(
            iter([f'event: {EventType.ERROR}\ndata: {{"error":"No dataset loaded"}}\n\n']),
            media_type="text/event-stream",
        )

    schema_parts = []
    total_rows = 0
    for t in state.schema_info:
        row_count = t.get("row_count") or 0
        total_rows += row_count
        cols = ", ".join(c["name"] for c in t.get("columns", [])[:10])
        if len(t.get("columns", [])) > 10:
            cols += f", ... ({len(t['columns'])} total)"
        schema_parts.append(f"- **{t['name']}** ({row_count:,} rows): {cols}")

    prompt = f"""Summarize this dataset in 2-3 short paragraphs. Describe:
1. What the data appears to represent (domain/purpose)
2. The key tables and how they might relate
3. What kinds of questions or analyses a user could explore

Be concise and helpful. Use markdown formatting.

## Dataset Schema
{total_rows:,} total rows across {len(state.schema_info)} tables:

{chr(10).join(schema_parts)}"""

    async def generate():
        if state.llm_client is None:
            yield f'event: {EventType.ERROR}\ndata: {{"error":"LLM not initialized"}}\n\n'
            return

        try:
            response = await state.llm_client.create_message(
                model=state.model,
                max_tokens=1024,
                system="You are a helpful data analyst. Provide clear, concise summaries.",
                tools=[],
                messages=[{"role": "user", "content": prompt}],
            )
            for block in response.content:
                if isinstance(block, TextBlock):
                    for word_i, word in enumerate(block.text.split(" ")):
                        chunk = word if word_i == 0 else " " + word
                        yield f"event: token\ndata: {json.dumps({'text': chunk})}\n\n"
            yield "event: done\ndata: {}\n\n"
        except Exception as e:
            logger.exception("Summarize error")
            yield f"event: {EventType.ERROR}\ndata: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/project")
async def get_current_project(state: AppState = Depends(get_state)):
    """Return current project info, or null if no project is loaded."""
    if state.is_ephemeral:
        return {
            "loaded": True,
            "path": None,
            "name": "Quick Explore",
            "is_ephemeral": True,
            "tables": state.ephemeral_tables_info,
            "sql_dialect": state.sql_dialect,
        }
    if not state.project_loaded or state.project_dir is None:
        return {
            "loaded": False,
            "path": None,
            "name": None,
            "is_ephemeral": False,
            "sql_dialect": state.sql_dialect,
        }
    return {
        "loaded": True,
        "path": state.project_dir,
        "name": get_project_name(state.project_dir),
        "is_ephemeral": False,
        "has_time_series": bool(state.time_series_configs),
        "sql_dialect": state.sql_dialect,
    }


@app.get("/api/projects/recent")
async def get_recent_projects(state: AppState = Depends(get_state)):
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
async def load_project_endpoint(request: Request, state: AppState = Depends(get_state)):
    """Load a project directory."""
    body = await request.json()
    project_path = body.get("path", "")

    async with state.state_lock:
        try:
            result = await load_project(project_path, state)
            return {"success": True, **result}
        except ProjectError as e:
            logger.error(f"Failed to load project {project_path}: {e}")
            return {"success": False, "error": str(e)}
        except Exception as e:
            logger.exception("Unexpected error loading project")
            return {"success": False, "error": str(e)}


@app.delete("/api/projects/recent/{project_path:path}")
async def remove_project_from_recent(project_path: str):
    """Remove a project from the recent list."""
    from urllib.parse import unquote

    decoded_path = unquote(project_path)
    if not decoded_path.startswith("/"):
        decoded_path = "/" + decoded_path
    remove_recent_project(decoded_path)
    return {"success": True}


# ---------------------------------------------------------------------------
# Explore endpoints (quick file exploration without project setup)
# ---------------------------------------------------------------------------


@app.post("/api/explore")
async def explore_files(request: Request, state: AppState = Depends(get_state)):
    """Create an ephemeral session from file paths.

    Request body:
        paths: list of file/directory paths to explore

    Returns:
        success: bool
        tables: list of table info dicts (name, path, type)
        error: optional error message
    """
    body = await request.json()
    file_paths = body.get("paths", [])

    if not file_paths:
        return {"success": False, "error": "No file paths provided"}

    try:
        # Clear any existing project/session
        state.clear_project()

        # Restore original env to prevent leaking settings from previous projects
        restore_original_env()

        # Load settings from environment (for LLM config, app preferences)
        settings = Settings.from_env(None)

        # Try to initialize LLM client (not required for data loading)
        init_llm_client(state)

        # Apply app settings
        state.confirm_sql = settings.app.confirm_sql
        state.explain_sql = settings.app.explain_sql
        state.clarify_sql = settings.app.clarify_sql
        state.show_provenance = settings.app.show_provenance
        state._max_history_pairs = settings.app.max_history_pairs
        state._response_cache_max = settings.app.response_cache_max
        state.max_cost_usd_per_turn = settings.app.max_cost_usd_per_turn
        state.max_output_tokens = settings.app.max_output_tokens

        # Route file-backed exploration through whichever backend the
        # current settings point at (Spark when DB_MODE=spark, else DuckDB).
        from datasight.explore import create_files_session_for_settings
        from datasight.runner import SparkConnectRunner

        runner, tables_info = create_files_session_for_settings(file_paths, settings.database)
        state.sql_runner = runner
        state.is_ephemeral = True
        state.ephemeral_tables_info = tables_info
        # Dialect must match the effective runner, not the configured DB_MODE:
        # for DB_MODE=postgres/flightsql/sqlite the file session falls back to
        # an in-memory DuckDB, so SQL must be generated as DuckDB rather than
        # the configured backend's dialect.
        state.sql_dialect = "spark" if isinstance(runner, SparkConnectRunner) else "duckdb"
        state.project_loaded = True

        # Introspect schema
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

        # Build schema context (no user description for ephemeral sessions)
        state.schema_text = format_schema_context(tables, user_description=None)
        state.schema_map = build_schema_map(state.schema_info)
        measure_text = format_measure_prompt_context(
            await build_measure_overview(
                state.schema_info, state.sql_runner.run_sql, overrides=None
            )
        )
        if measure_text:
            state.schema_text += measure_text

        # Build system prompt (use rebuild to get mode="web")
        state.rebuild_system_prompt()

        # Set up ephemeral storage (in-memory only)
        state.conversations = None
        state.bookmarks = None
        state.dashboard = None
        state.query_logger = None
        state.example_queries_list = []

        logger.info(f"Created ephemeral session with {len(tables_info)} tables")

        return {
            "success": True,
            "tables": tables_info,
            "schema_info": state.schema_info,
            "llm_connected": state.llm_client is not None and _has_llm_api_key(),
        }

    except ConfigurationError as e:
        logger.error(f"Failed to create ephemeral session: {e}")
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.exception("Unexpected error creating ephemeral session")
        return {"success": False, "error": str(e)}


@app.post("/api/explore/check-project-path")
async def check_project_path(request: Request):
    """Check if a project directory already contains project files.

    Returns which files already exist so the frontend can prompt to overwrite.
    """
    body = await request.json()
    project_path = body.get("path", "")
    if not project_path:
        return {"exists": False, "files": []}

    dest = Path(project_path).resolve()
    existing: list[str] = []
    for name in (".env", "schema_description.md", "queries.yaml", "data.duckdb"):
        if (dest / name).exists():
            existing.append(name)

    return {"exists": bool(existing), "files": existing}


@app.post("/api/explore/save-project")
async def save_explore_as_project(request: Request, state: AppState = Depends(get_state)):
    """Save the current ephemeral session as a project.

    Request body:
        path: directory path to create the project in
        name: optional project name

    Returns:
        success: bool
        path: path to the created project
        error: optional error message
    """
    if not state.is_ephemeral:
        return {"success": False, "error": "No ephemeral session active"}

    body = await request.json()
    project_path = body.get("path", "")
    project_name = body.get("name")

    if not project_path:
        return {"success": False, "error": "Project path is required"}

    try:
        # Save as project
        saved_path = save_ephemeral_as_project(
            runner=state.sql_runner,
            tables_info=state.ephemeral_tables_info,
            project_dir=project_path,
            project_name=project_name,
        )
        await _write_measure_overrides_scaffold(saved_path, state.schema_info, state.sql_runner)

        # Load the newly created project
        result = await load_project(saved_path, state)

        return {"success": True, "path": saved_path, **result}

    except Exception as e:
        logger.exception("Failed to save project")
        return {"success": False, "error": str(e)}


@app.post("/api/add-files")
async def add_files_endpoint(request: Request, state: AppState = Depends(get_state)):
    """Add files/tables to the current session (explore or project).

    Request body:
        paths: list of file/directory paths to add

    Returns:
        success: bool
        added: list of table info dicts for newly created views
        error: optional error message
    """
    body = await request.json()
    file_paths = body.get("paths", [])

    if not file_paths:
        return {"success": False, "error": "No file paths provided"}
    if state.sql_runner is None:
        return {"success": False, "error": "No data loaded"}

    try:
        # Get existing table names to avoid collisions
        existing_names = {t["name"] for t in state.schema_info}

        # Get a writable DuckDB connection
        import duckdb as _duckdb

        from datasight.runner import DuckDBRunner, EphemeralDuckDBRunner

        runner = state.sql_runner
        if isinstance(runner, CachingSqlRunner):
            runner = runner._inner
        conn: _duckdb.DuckDBPyConnection | None = None
        reopen_readonly = False

        if isinstance(runner, EphemeralDuckDBRunner) and runner._conn is not None:
            conn = runner._conn
        elif isinstance(runner, DuckDBRunner):
            db_path = runner._database_path
            # Cached DataFrames can keep DuckDB buffers alive and block a
            # read-write reopen of the same file; drop them before closing.
            state.clear_sql_cache()
            runner.close()
            import gc

            gc.collect()
            conn = _duckdb.connect(db_path, read_only=False)
            reopen_readonly = True

        if conn is None:
            return {"success": False, "error": "Cannot add files to this database type"}

        new_tables = add_files_to_connection(conn, file_paths, existing_names)

        # For project mode DuckDB, close writable connection and reopen read-only
        if reopen_readonly and isinstance(runner, DuckDBRunner):
            conn.close()
            runner._connect()

        # Update state
        if state.is_ephemeral:
            state.ephemeral_tables_info.extend(new_tables)

        # New tables invalidate any cached schema/result queries.
        state.clear_sql_cache()

        # Re-introspect schema
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
        state.schema_text = format_schema_context(
            tables,
            user_description=None if state.is_ephemeral else _load_user_description(state),
        )
        state.schema_map = build_schema_map(state.schema_info)
        state.rebuild_system_prompt()

        logger.info(f"Added {len(new_tables)} table(s) to session")

        return {
            "success": True,
            "added": new_tables,
            "schema_info": state.schema_info,
        }

    except ConfigurationError as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.exception("Failed to add files")
        return {"success": False, "error": str(e)}


def _load_user_description(state: AppState) -> str | None:
    """Load user schema description for the current project."""
    if state.project_dir:
        return load_schema_description(
            os.environ.get("SCHEMA_DESCRIPTION_PATH"), state.project_dir
        )
    return None


def _normalize_measure_override_text(text: str) -> str:
    normalized = text if text.endswith("\n") else text + "\n"
    return normalized


def _validate_measure_override_entries(
    entries: list[dict[str, Any]],
    schema_info: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    table_map = {str(table["name"]): table for table in schema_info}
    seen: set[tuple[str, str]] = set()
    valid_aggregations = {"sum", "avg", "min", "max"}
    valid_average_strategies = {"avg", "weighted_avg"}
    valid_formats = {"currency", "percent", "integer", "float", "decimal", "mw", "mwh", "kwh"}
    valid_chart_types = {"line", "bar", "area", "scatter", "heatmap"}

    for index, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            errors.append(f"Entry {index} must be a mapping.")
            continue

        table_name = str(entry.get("table") or "")
        column_name = str(entry.get("column") or entry.get("name") or "")
        expression = str(entry.get("expression") or entry.get("sql_expression") or "")
        label = f"Entry {index}"

        if not table_name:
            errors.append(f"{label} is missing `table`.")
            continue
        if not column_name:
            errors.append(f"{label} is missing `column` or `name`.")
            continue

        key = (table_name, column_name)
        if key in seen:
            warnings.append(
                f"Duplicate override for {table_name}.{column_name}; later entries win."
            )
        seen.add(key)

        table = table_map.get(table_name)
        if table is None:
            errors.append(f"{table_name}.{column_name}: table not found.")
            continue

        columns = {str(column["name"]): column for column in table.get("columns", [])}
        has_physical_column = column_name in columns
        if not has_physical_column and not expression:
            errors.append(f"{table_name}.{column_name}: column not found.")
            continue
        if expression and not str(entry.get("name") or "").strip() and not entry.get("column"):
            errors.append(f"{table_name}.{column_name}: calculated measures must include `name`.")
            continue

        default_aggregation = entry.get("default_aggregation")
        if default_aggregation and str(default_aggregation) not in valid_aggregations:
            errors.append(
                f"{table_name}.{column_name}: invalid default_aggregation `{default_aggregation}`."
            )

        average_strategy = entry.get("average_strategy")
        if average_strategy and str(average_strategy) not in valid_average_strategies:
            errors.append(
                f"{table_name}.{column_name}: invalid average_strategy `{average_strategy}`."
            )

        display_name = entry.get("display_name")
        if display_name is not None and not str(display_name).strip():
            errors.append(f"{table_name}.{column_name}: display_name cannot be blank.")

        fmt = entry.get("format")
        if fmt and str(fmt) not in valid_formats:
            errors.append(f"{table_name}.{column_name}: invalid format `{fmt}`.")

        weight_column = str(entry.get("weight_column") or "")
        if weight_column and weight_column not in columns:
            errors.append(
                f"{table_name}.{column_name}: weight_column `{weight_column}` not found."
            )

        allowed = entry.get("allowed_aggregations") or []
        if allowed and not isinstance(allowed, list):
            errors.append(f"{table_name}.{column_name}: allowed_aggregations must be a list.")
        elif any(str(value) not in valid_aggregations for value in allowed):
            errors.append(
                f"{table_name}.{column_name}: allowed_aggregations contains invalid values."
            )

        forbidden = entry.get("forbidden_aggregations") or []
        if forbidden and not isinstance(forbidden, list):
            errors.append(f"{table_name}.{column_name}: forbidden_aggregations must be a list.")
        elif any(str(value) not in valid_aggregations for value in forbidden):
            errors.append(
                f"{table_name}.{column_name}: forbidden_aggregations contains invalid values."
            )

        chart_types = entry.get("preferred_chart_types") or []
        if chart_types and not isinstance(chart_types, list):
            errors.append(f"{table_name}.{column_name}: preferred_chart_types must be a list.")
        elif any(str(value) not in valid_chart_types for value in chart_types):
            errors.append(
                f"{table_name}.{column_name}: preferred_chart_types contains invalid values."
            )

    return errors, warnings


def _serialize_measure_override_entries(entries: list[dict[str, Any]]) -> str:
    return format_measure_overrides_yaml({"measures": entries})


async def _write_measure_overrides_scaffold(
    project_dir: str,
    schema_info: list[dict[str, Any]],
    sql_runner: SqlRunner | None,
) -> str | None:
    """Seed measures.yaml from inferred measures if it does not already exist."""
    if sql_runner is None:
        return None

    path = Path(project_dir) / "measures.yaml"
    if path.exists():
        return None

    measure_data = await build_measure_overview(schema_info, sql_runner.run_sql, overrides=None)
    path.write_text(format_measure_overrides_yaml(measure_data), encoding="utf-8")
    return path.name


@app.get("/api/explore/scan-cwd")
async def explore_scan_cwd(state: AppState = Depends(get_state)):
    """Scan the server's working directory for CSV/Parquet/Excel files.

    Returns an empty list once a project or ephemeral session is loaded so
    the landing page only surfaces discovered files on a fresh launch.
    """
    if state.project_loaded:
        return {"directory": str(Path.cwd()), "files": [], "truncated": False}

    directory = Path.cwd()
    files, truncated = scan_directory_for_data_files(directory)
    return {
        "directory": str(directory),
        "files": files,
        "truncated": truncated,
    }


@app.get("/api/explore/status")
async def explore_status(state: AppState = Depends(get_state)):
    """Get the current explore session status."""
    return {
        "is_ephemeral": state.is_ephemeral,
        "tables": state.ephemeral_tables_info if state.is_ephemeral else [],
        "project_loaded": state.project_loaded,
        "project_dir": state.project_dir,
    }


@app.post("/api/explore/generate-project")
async def generate_project(request: Request, state: AppState = Depends(get_state)):
    """Save ephemeral session as a project with LLM-generated documentation.

    Streams SSE events for progress, token output, and completion.

    Request body:
        path: project directory
        name: optional project name
        description: optional user description of the data
    """
    body = await request.json()
    project_path = body.get("path", "")
    project_name = body.get("name")
    user_description = body.get("description")

    async def generate():
        try:
            # Validate state
            if state.sql_runner is None:
                yield _sse("error", {"error": "No data loaded"})
                return
            if not project_path:
                yield _sse("error", {"error": "Project path is required"})
                return
            if state.llm_client is None:
                yield _sse("error", {"error": "LLM client not configured. Check API key."})
                return

            # Step 1: Save base project structure
            yield _sse("status", {"step": "saving", "message": "Creating project structure..."})
            saved_path = save_ephemeral_as_project(
                runner=state.sql_runner,
                tables_info=state.ephemeral_tables_info,
                project_dir=project_path,
                project_name=project_name,
            )

            # Step 2: Sample enum + timestamp columns
            yield _sse("status", {"step": "sampling", "message": "Sampling column values..."})
            tables = await introspect_schema(state.sql_runner.run_sql, runner=state.sql_runner)
            samples_text = await sample_enum_columns(state.sql_runner.run_sql, tables)
            timestamps_text = await sample_timestamp_columns(state.sql_runner.run_sql, tables)

            # Step 3: Call LLM to generate documentation
            yield _sse("status", {"step": "generating", "message": "Generating documentation..."})
            system_prompt, user_msg = build_generation_context(
                tables,
                state.sql_dialect,
                samples_text,
                user_description=user_description,
                timestamps_text=timestamps_text,
            )

            response = await state.llm_client.create_message(
                model=state.model,
                system=system_prompt,
                messages=[{"role": "user", "content": user_msg}],
                tools=[],
                max_tokens=4096,
            )

            # Stream tokens from response
            full_text = ""
            for block in response.content:
                if isinstance(block, TextBlock):
                    full_text += block.text
                    for word_i, word in enumerate(block.text.split(" ")):
                        chunk = word if word_i == 0 else " " + word
                        yield _sse("token", {"text": chunk})

            # Step 4: Parse and write files
            yield _sse("status", {"step": "writing", "message": "Writing project files..."})
            schema_content, queries_content = parse_generation_response(full_text)

            project_dir_path = Path(saved_path)
            files_written = []
            if schema_content:
                (project_dir_path / "schema_description.md").write_text(
                    schema_content + "\n", encoding="utf-8"
                )
                files_written.append("schema_description.md")
            if queries_content:
                (project_dir_path / "queries.yaml").write_text(
                    queries_content + "\n", encoding="utf-8"
                )
                files_written.append("queries.yaml")
            measures_file = await _write_measure_overrides_scaffold(
                saved_path,
                state.schema_info,
                state.sql_runner,
            )
            if measures_file:
                files_written.append(measures_file)

            # Step 5: Load the project
            yield _sse("status", {"step": "loading", "message": "Loading project..."})
            await load_project(saved_path, state)

            yield _sse(
                "done",
                {
                    "path": saved_path,
                    "name": project_name or Path(saved_path).name,
                    "files": files_written,
                },
            )

        except Exception as e:
            logger.exception("Generate project error")
            yield _sse("error", {"error": str(e)})

    return StreamingResponse(generate(), media_type="text/event-stream")


def _sse(event: str, data: dict) -> str:
    """Format a single SSE event."""
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


@app.get("/api/preview/{table_name}")
async def preview_table(table_name: str, state: AppState = Depends(get_state)):
    """Return an HTML table preview of the first 10 rows."""
    valid_names = {t["name"] for t in state.schema_info}
    if table_name not in valid_names:
        return {"html": None, "error": "Unknown table"}
    if state.sql_runner is None:
        return {"html": None, "error": "Database not connected"}
    try:
        cache_key = f"preview:{table_name}"
        cached = state.get_insight_cache(cache_key)
        if cached is not None:
            logger.debug(f"[insight-cache] HIT {cache_key}")
            return {"html": cached, "cached": True}

        df = await state.sql_runner.run_sql(f'SELECT * FROM "{table_name}" LIMIT 10')
        html = df_to_html_table(df, max_rows=10)
        state.put_insight_cache(cache_key, html)
        logger.debug(f"[insight-cache] STORED {cache_key}")
        return {"html": html, "cached": False}
    except Exception as e:
        logger.debug(f"Preview error: {e}")
        return {"html": None, "error": str(e)}


@app.get("/api/column-stats/{table_name}/{column_name}")
async def column_stats(table_name: str, column_name: str, state: AppState = Depends(get_state)):
    """Return basic statistics for a column."""
    valid_tables = {t["name"] for t in state.schema_info}
    if table_name not in valid_tables:
        return {"stats": None, "error": "Unknown table"}

    table_info = next((t for t in state.schema_info if t["name"] == table_name), None)
    if not table_info:
        return {"stats": None, "error": "Unknown table"}

    valid_cols = {c["name"] for c in table_info["columns"]}
    if column_name not in valid_cols:
        return {"stats": None, "error": "Unknown column"}

    if state.sql_runner is None:
        return {"stats": None, "error": "Database not connected"}

    col_info = next(c for c in table_info["columns"] if c["name"] == column_name)
    try:
        cache_key = f"column-stats:{table_name}.{column_name}"
        cached = state.get_insight_cache(cache_key)
        if cached is not None:
            logger.debug(f"[insight-cache] HIT {cache_key}")
            return {"stats": cached, "cached": True}

        dtype = col_info["dtype"].lower()
        is_numeric = any(
            t in dtype for t in ("int", "float", "double", "decimal", "numeric", "real")
        )
        if is_numeric:
            if state.sql_dialect == "sqlite":
                avg_expr = f'ROUND(AVG("{column_name}"), 2)'
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
        for k, v in stats.items():
            if hasattr(v, "item"):
                stats[k] = v.item()
            elif hasattr(v, "isoformat"):
                stats[k] = str(v)
        state.put_insight_cache(cache_key, stats)
        logger.debug(f"[insight-cache] STORED {cache_key}")
        return {"stats": stats, "cached": False}
    except Exception as e:
        logger.debug(f"Column stats error: {e}")
        return {"stats": None, "error": str(e)}


@app.get("/api/query-log")
async def get_query_log(n: int = 50, state: AppState = Depends(get_state)):
    """Return recent query log entries."""
    if state.query_logger is None:
        return {"entries": []}
    return {"entries": state.query_logger.read_recent(n)}


@app.post("/api/sql-execute")
async def sql_execute(request: Request, state: AppState = Depends(get_state)):
    """Execute user-authored SQL from the SQL editor page."""
    body = await request.json()
    sql = str(body.get("sql") or "").strip()
    session_id = str(body.get("session_id") or "")
    if not sql:
        return {"html": None, "row_count": 0, "elapsed_ms": 0.0, "error": "SQL is empty"}
    if state.sql_runner is None:
        return {"html": None, "row_count": 0, "elapsed_ms": 0.0, "error": "Database not connected"}

    started = time.perf_counter()
    try:
        df = await state.sql_runner.run_sql(sql)
    except Exception as e:
        elapsed = (time.perf_counter() - started) * 1000
        if state.query_logger is not None:
            state.query_logger.log(
                session_id=session_id,
                user_question="(SQL editor)",
                tool="sql_editor",
                sql=sql,
                execution_time_ms=elapsed,
                error=str(e),
            )
        return {"html": None, "row_count": 0, "elapsed_ms": elapsed, "error": str(e)}
    elapsed = (time.perf_counter() - started) * 1000
    if state.query_logger is not None:
        state.query_logger.log(
            session_id=session_id,
            user_question="(SQL editor)",
            tool="sql_editor",
            sql=sql,
            execution_time_ms=elapsed,
            row_count=int(len(df)),
            column_count=int(len(df.columns)),
        )
    return {
        "html": df_to_html_table(df),
        "row_count": int(len(df)),
        "elapsed_ms": elapsed,
        "error": None,
    }


@app.post("/api/sql-validate")
async def sql_validate(request: Request, state: AppState = Depends(get_state)):
    """Validate user-authored SQL with sqlglot (table refs + parse check)."""
    body = await request.json()
    sql = str(body.get("sql") or "").strip()
    if not sql:
        return {"valid": True, "errors": []}
    schema_map = build_schema_map(state.schema_info)
    result = validate_sql(
        sql,
        schema_map,
        dialect=state.sql_dialect,
        measure_rules=state.measure_rules or None,
    )
    return {"valid": result.valid, "errors": result.errors}


@app.post("/api/sql-format")
async def sql_format(request: Request, state: AppState = Depends(get_state)):
    """Pretty-print user-authored SQL with sqlglot."""
    body = await request.json()
    sql = str(body.get("sql") or "")
    if not sql.strip():
        return {"formatted": sql, "error": None}
    try:
        import sqlglot

        statements = sqlglot.transpile(
            sql,
            read=state.sql_dialect,
            write=state.sql_dialect,
            pretty=True,
        )
        formatted = ";\n\n".join(statements)
        return {"formatted": formatted, "error": None}
    except Exception as e:
        return {"formatted": sql, "error": str(e)}


@app.get("/api/measures/editor")
async def get_measure_overrides_editor(state: AppState = Depends(get_state)):
    """Return editable measure override YAML for the active project."""
    if not state.project_loaded or not state.project_dir or state.sql_runner is None:
        return {"ok": False, "error": "No project loaded"}

    path = Path(state.project_dir) / "measures.yaml"
    if path.exists():
        return {"ok": True, "text": path.read_text(encoding="utf-8"), "path": str(path)}

    measure_data = await build_measure_overview(
        state.schema_info,
        state.sql_runner.run_sql,
        load_measure_overrides(None, state.project_dir),
    )
    return {
        "ok": True,
        "text": format_measure_overrides_yaml(measure_data),
        "path": str(path),
        "generated": True,
    }


@app.get("/api/measures/editor/catalog")
async def get_measure_overrides_catalog(state: AppState = Depends(get_state)):
    """Return inferred measure metadata for the structured override editor."""
    if not state.project_loaded or not state.project_dir or state.sql_runner is None:
        return {"ok": False, "error": "No project loaded", "measures": []}

    measure_data = await build_measure_overview(
        state.schema_info,
        state.sql_runner.run_sql,
        load_measure_overrides(None, state.project_dir),
    )
    return {"ok": True, "measures": measure_data.get("measures", [])}


@app.post("/api/measures/editor/validate")
async def validate_measure_overrides_editor(
    request: Request, state: AppState = Depends(get_state)
):
    """Validate measure override YAML for the active project."""
    if not state.project_loaded or not state.project_dir:
        return {"ok": False, "error": "No project loaded", "errors": ["No project loaded"]}

    body = await request.json()
    text = str(body.get("text") or "")
    try:
        parsed = yaml.safe_load(text) if text.strip() else []
    except yaml.YAMLError as exc:
        return {"ok": False, "errors": [f"Invalid YAML: {exc}"], "warnings": []}

    if parsed is not None and not isinstance(parsed, list):
        return {
            "ok": False,
            "errors": ["measures.yaml must contain a YAML list."],
            "warnings": [],
        }

    entries = parsed or []
    errors, warnings = _validate_measure_override_entries(entries, state.schema_info)
    return {"ok": not errors, "errors": errors, "warnings": warnings}


@app.post("/api/measures/editor/upsert")
async def upsert_measure_override_editor(request: Request, state: AppState = Depends(get_state)):
    """Upsert a single measure override entry into the current editor text."""
    if not state.project_loaded or not state.project_dir:
        return {"ok": False, "error": "No project loaded"}

    body = await request.json()
    text = str(body.get("text") or "")
    try:
        parsed = yaml.safe_load(text) if text.strip() else []
    except yaml.YAMLError as exc:
        return {"ok": False, "error": f"Invalid YAML: {exc}"}
    if parsed is not None and not isinstance(parsed, list):
        return {"ok": False, "error": "measures.yaml must contain a YAML list."}

    table_name = str(body.get("table") or "").strip()
    column_name = str(body.get("column") or "").strip()
    measure_name = str(body.get("name") or "").strip()
    expression = str(body.get("expression") or body.get("sql_expression") or "").strip()
    if not table_name:
        return {"ok": False, "error": "Table is required."}
    if not column_name and not (measure_name and expression):
        return {
            "ok": False,
            "error": "Provide either a physical column or a calculated measure name and expression.",
        }

    entry: dict[str, Any] = {
        "table": table_name,
        "default_aggregation": str(body.get("default_aggregation") or "avg"),
        "average_strategy": str(body.get("average_strategy") or "avg"),
    }
    display_name = str(body.get("display_name") or "").strip()
    fmt = str(body.get("format") or "").strip()
    preferred_chart_types = body.get("preferred_chart_types") or []
    if column_name:
        entry["column"] = column_name
    if measure_name:
        entry["name"] = measure_name
    if expression:
        entry["expression"] = expression
    if display_name:
        entry["display_name"] = display_name
    if fmt:
        entry["format"] = fmt
    if isinstance(preferred_chart_types, list) and preferred_chart_types:
        entry["preferred_chart_types"] = [str(item) for item in preferred_chart_types if str(item)]
    weight_column = str(body.get("weight_column") or "").strip()
    if weight_column:
        entry["weight_column"] = weight_column

    existing_entries = [item for item in (parsed or []) if isinstance(item, dict)]
    updated = False
    for index, existing in enumerate(existing_entries):
        existing_name = str(existing.get("column") or existing.get("name") or "")
        target_name = column_name or measure_name
        if str(existing.get("table") or "") == table_name and existing_name == target_name:
            merged = dict(existing)
            merged.update(entry)
            if not weight_column and "weight_column" in merged:
                merged.pop("weight_column", None)
            existing_entries[index] = merged
            updated = True
            break
    if not updated:
        existing_entries.append(entry)

    errors, warnings = _validate_measure_override_entries(existing_entries, state.schema_info)
    if errors:
        return {"ok": False, "error": errors[0], "errors": errors, "warnings": warnings}

    return {
        "ok": True,
        "text": _serialize_measure_override_entries(existing_entries),
        "warnings": warnings,
    }


@app.post("/api/measures/editor")
async def save_measure_overrides_editor(request: Request, state: AppState = Depends(get_state)):
    """Save measure override YAML for the active project."""
    if not state.project_loaded or not state.project_dir:
        return {"ok": False, "error": "No project loaded"}

    body = await request.json()
    text = str(body.get("text") or "")
    try:
        parsed = yaml.safe_load(text) if text.strip() else []
    except yaml.YAMLError as exc:
        return {"ok": False, "error": f"Invalid YAML: {exc}"}
    if parsed is not None and not isinstance(parsed, list):
        return {"ok": False, "error": "measures.yaml must contain a YAML list."}

    entries = parsed or []
    errors, warnings = _validate_measure_override_entries(entries, state.schema_info)
    if errors:
        return {"ok": False, "error": errors[0], "errors": errors, "warnings": warnings}

    project_dir = state.project_dir
    path = Path(project_dir) / "measures.yaml"
    path.write_text(_normalize_measure_override_text(text), encoding="utf-8")

    async with state.state_lock:
        await load_project(project_dir, state)

    return {"ok": True, "path": str(path), "warnings": warnings}


@app.get("/api/bookmarks")
async def list_bookmarks(state: AppState = Depends(get_state)):
    """Return all bookmarked queries."""
    if state.bookmarks is None:
        return {"bookmarks": []}
    return {"bookmarks": state.bookmarks.list_all()}


@app.post("/api/bookmarks")
async def add_bookmark(request: Request, state: AppState = Depends(get_state)):
    """Add a bookmarked query."""
    if state.bookmarks is None:
        return {"ok": False, "error": "No project loaded"}
    body = await request.json()
    sql = body.get("sql", "").strip()
    tool = body.get("tool", "run_sql")
    name = body.get("name", "").strip()
    if not sql:
        return {"error": "sql is required"}
    bookmark = state.bookmarks.add(sql, tool, name)
    return {"bookmark": bookmark}


@app.delete("/api/bookmarks/{bookmark_id}")
async def remove_bookmark(bookmark_id: int, state: AppState = Depends(get_state)):
    """Remove a bookmarked query."""
    if state.bookmarks is None:
        return {"ok": True}
    state.bookmarks.delete(bookmark_id)
    return {"ok": True}


@app.delete("/api/bookmarks")
async def clear_bookmarks(state: AppState = Depends(get_state)):
    """Remove all bookmarks."""
    if state.bookmarks is None:
        return {"ok": True}
    state.bookmarks.clear()
    return {"ok": True}


@app.get("/api/dashboard")
async def get_dashboard(request: Request, state: AppState = Depends(get_state)):
    """Return all dashboard items and layout settings."""
    session_id = request.query_params.get("session_id")
    if session_id and state.conversations is not None:
        try:
            conv = state.conversations.get(validate_session_id(session_id))
        except InvalidSessionIdError:
            return _empty_dashboard()
        dashboard = conv.get("dashboard")
        if isinstance(dashboard, dict):
            return {
                "items": dashboard.get("items", []),
                "columns": dashboard.get("columns", 0),
                "filters": dashboard.get("filters", []),
                "title": dashboard.get("title", "") or "",
            }
    if state.dashboard is None:
        return _empty_dashboard()
    return state.dashboard.get_all()


@app.post("/api/dashboard")
async def save_dashboard(request: Request, state: AppState = Depends(get_state)):
    """Save dashboard items and layout settings."""
    if state.dashboard is None:
        return _empty_dashboard()
    body = await request.json()
    items = body.get("items", [])
    columns = body.get("columns")
    filters = body.get("filters")
    title = body.get("title")
    result = state.dashboard.save_all(items, columns, filters, title)
    session_id = body.get("session_id")
    if session_id and state.conversations is not None:
        try:
            conv = state.conversations.get(validate_session_id(str(session_id)))
            conv["dashboard"] = result
            await state.save_session(str(session_id))
        except InvalidSessionIdError:
            pass
    return result


@app.delete("/api/dashboard")
async def clear_dashboard(state: AppState = Depends(get_state)):
    """Clear all dashboard items."""
    if state.dashboard is None:
        return {"ok": True}
    state.dashboard.clear()
    return {"ok": True}


@app.post("/api/dashboard/run-card")
async def run_dashboard_card(request: Request, state: AppState = Depends(get_state)):
    """Re-execute a dashboard card with post-aggregation result filters."""
    if state.sql_runner is None:
        return {"ok": False, "error": "SQL runner not initialized"}

    body = await request.json()
    sql = str(body.get("sql") or "").strip()
    if not sql:
        return {"ok": False, "error": "sql is required"}

    tool = str(body.get("tool") or "run_sql")
    if tool not in {"run_sql", "visualize_data"}:
        return {"ok": False, "error": f"Unsupported dashboard card tool: {tool}"}

    filters = body.get("filters") or []
    if not isinstance(filters, list):
        return {"ok": False, "error": "filters must be a list"}
    allowed_columns = body.get("allowed_columns") or []
    if allowed_columns:
        if not isinstance(allowed_columns, list):
            return {"ok": False, "error": "allowed_columns must be a list"}
        allowed_column_set = {str(column) for column in allowed_columns}
        disallowed = [
            str(filter_data.get("column"))
            for filter_data in filters
            if isinstance(filter_data, dict)
            and filter_data.get("column")
            and str(filter_data.get("column")) not in allowed_column_set
        ]
        if disallowed:
            return {
                "ok": False,
                "error": (
                    "Dashboard filter column is not shared across runnable cards: "
                    + ", ".join(disallowed)
                ),
            }

    try:
        filtered_sql = _apply_dashboard_filters(sql, filters)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}

    tool_input: dict[str, Any] = {
        "sql": filtered_sql,
        "title": body.get("title") or "Dashboard card",
    }
    if tool == "visualize_data":
        tool_input["plotly_spec"] = body.get("plotly_spec") or {}

    result = await execute_tool(
        tool,
        tool_input,
        run_sql=state.sql_runner.run_sql,
        schema_map=state.schema_map or None,
        dialect=state.sql_dialect,
        measure_rules=state.measure_rules or None,
        query_logger=state.query_logger,
        session_id=str(body.get("session_id") or ""),
        user_question="[Dashboard filter]",
    )

    is_chart = tool == "visualize_data" and result.result_html and "<script" in result.result_html
    return {
        "ok": not bool(result.meta.get("error")),
        "html": result.result_html,
        "type": "chart" if is_chart else "table",
        "title": body.get("title") or "",
        "meta": result.meta,
        "sql": filtered_sql,
        "plotly_spec": result.plotly_spec,
        "error": result.meta.get("error"),
    }


@app.post("/api/dashboard/filter-values")
async def get_dashboard_filter_values(request: Request, state: AppState = Depends(get_state)):
    """Return distinct post-aggregation values for a dashboard filter column."""
    if state.sql_runner is None:
        return {"ok": False, "error": "SQL runner not initialized", "values": []}

    body = await request.json()
    column = str(body.get("column") or "").strip()
    if not column:
        return {"ok": False, "error": "column is required", "values": []}
    try:
        quoted_column = _quote_dashboard_filter_identifier(column)
    except ValueError as exc:
        return {"ok": False, "error": str(exc), "values": []}

    allowed_columns = body.get("allowed_columns") or []
    if allowed_columns:
        if not isinstance(allowed_columns, list):
            return {"ok": False, "error": "allowed_columns must be a list", "values": []}
        allowed_column_set = {str(item) for item in allowed_columns}
        if column not in allowed_column_set:
            return {
                "ok": False,
                "error": f"Dashboard filter column is not shared across runnable cards: {column}",
                "values": [],
            }

    items = body.get("items") or []
    if not isinstance(items, list):
        return {"ok": False, "error": "items must be a list", "values": []}
    filters = body.get("filters") or []
    if not isinstance(filters, list):
        return {"ok": False, "error": "filters must be a list", "values": []}

    try:
        limit = int(body.get("limit") or 100)
    except (TypeError, ValueError):
        limit = 100
    limit = max(1, min(limit, 100))

    values: list[Any] = []
    comparison_filters = [
        item
        for item in filters
        if isinstance(item, dict) and str(item.get("column") or "").strip() != column
    ]
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("type") not in {"chart", "table"}:
            continue
        sql = str(item.get("sql") or "").strip().rstrip(";")
        if not sql:
            continue

        try:
            filtered_sql = _apply_dashboard_filters(sql, comparison_filters)
        except ValueError as exc:
            return {"ok": False, "error": str(exc), "values": []}

        distinct_sql = (
            f"SELECT DISTINCT {quoted_column} AS value\n"
            "FROM (\n"
            f"{filtered_sql}\n"
            ") AS datasight_dashboard_values\n"
            f"WHERE {quoted_column} IS NOT NULL\n"
            f"ORDER BY {quoted_column}\n"
            f"LIMIT {limit}"
        )
        try:
            df = await state.sql_runner.run_sql(distinct_sql)
        except Exception as exc:
            return {"ok": False, "error": str(exc), "values": []}
        if "value" in df.columns:
            values.extend(df["value"].tolist())
        values = _normalize_dashboard_filter_values(values, limit)
        if len(values) >= limit:
            break

    return {"ok": True, "values": values, "limit": limit}


@app.get("/api/reports")
async def list_reports(state: AppState = Depends(get_state)):
    """Return all saved reports."""
    if state.reports is None:
        return {"reports": []}
    return {"reports": state.reports.list_all()}


@app.post("/api/reports")
async def add_report(request: Request, state: AppState = Depends(get_state)):
    """Save a new report."""
    if state.reports is None:
        return {"ok": False, "error": "No project loaded"}
    body = await request.json()
    sql = body.get("sql", "").strip()
    tool = body.get("tool", "run_sql")
    name = body.get("name", "").strip()
    plotly_spec = body.get("plotly_spec")
    if not sql:
        return {"error": "sql is required"}
    report = state.reports.add(sql, tool, name, plotly_spec)
    return {"report": report}


@app.patch("/api/reports/{report_id}")
async def update_report(report_id: int, request: Request, state: AppState = Depends(get_state)):
    """Update a saved report's SQL, name, or plotly_spec."""
    if state.reports is None:
        return {"ok": False, "error": "No project loaded"}
    body = await request.json()
    fields: dict[str, Any] = {}
    if "sql" in body:
        sql = body["sql"].strip()
        if not sql:
            return {"ok": False, "error": "sql cannot be empty"}
        fields["sql"] = sql
    if "name" in body:
        fields["name"] = body["name"].strip()
    if "plotly_spec" in body:
        fields["plotly_spec"] = body["plotly_spec"]
    if not fields:
        return {"ok": False, "error": "No fields to update"}
    updated = state.reports.update(report_id, fields)
    if updated is None:
        return {"ok": False, "error": "Report not found"}
    return {"ok": True, "report": updated}


@app.delete("/api/reports/{report_id}")
async def remove_report(report_id: int, state: AppState = Depends(get_state)):
    """Remove a saved report."""
    if state.reports is None:
        return {"ok": True}
    state.reports.delete(report_id)
    return {"ok": True}


@app.delete("/api/reports")
async def clear_reports(state: AppState = Depends(get_state)):
    """Remove all saved reports."""
    if state.reports is None:
        return {"ok": True}
    state.reports.clear()
    return {"ok": True}


@app.post("/api/reports/{report_id}/run")
async def run_report(report_id: int, state: AppState = Depends(get_state)):
    """Re-execute a saved report against fresh data."""
    if state.reports is None:
        return {"ok": False, "error": "No project loaded"}
    report = state.reports.get(report_id)
    if report is None:
        return {"ok": False, "error": "Report not found"}
    if state.sql_runner is None:
        return {"ok": False, "error": "SQL runner not initialized"}

    result = await execute_tool(
        report["tool"],
        {
            "sql": report["sql"],
            "title": report.get("name", "Report"),
            **({"plotly_spec": report["plotly_spec"]} if "plotly_spec" in report else {}),
        },
        run_sql=state.sql_runner.run_sql,
        schema_map=state.schema_map or None,
        dialect=state.sql_dialect,
        measure_rules=state.measure_rules or None,
        query_logger=state.query_logger,
        session_id="",
        user_question=f"[Report] {report.get('name', '')}",
    )

    is_chart = (
        report["tool"] == "visualize_data"
        and result.result_html
        and "<script" in result.result_html
    )
    return {
        "ok": True,
        "html": result.result_html,
        "type": "chart" if is_chart else "table",
        "title": report.get("name", ""),
        "meta": result.meta,
        "plotly_spec": report.get("plotly_spec"),
    }


@app.get("/api/settings")
async def get_settings(state: AppState = Depends(get_state)):
    """Return current feature toggles."""
    return {
        "confirm_sql": state.confirm_sql,
        "explain_sql": state.explain_sql,
        "clarify_sql": state.clarify_sql,
        "show_cost": state.show_cost,
        "show_provenance": state.show_provenance,
    }


@app.post("/api/settings")
async def update_settings(request: Request, state: AppState = Depends(get_state)):
    """Update feature toggles."""
    body = await request.json()
    async with state.state_lock:
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
        if "show_cost" in body:
            state.show_cost = bool(body["show_cost"])
        if "show_provenance" in body:
            state.show_provenance = bool(body["show_provenance"])
        if need_rebuild:
            state.rebuild_system_prompt()
        else:
            state.clear_insight_cache()
    logger.info(
        f"Settings updated: confirm_sql={state.confirm_sql}, "
        f"explain_sql={state.explain_sql}, clarify_sql={state.clarify_sql}, "
        f"show_cost={state.show_cost}, show_provenance={state.show_provenance}"
    )
    return {
        "confirm_sql": state.confirm_sql,
        "explain_sql": state.explain_sql,
        "clarify_sql": state.clarify_sql,
        "show_cost": state.show_cost,
        "show_provenance": state.show_provenance,
    }


def _has_llm_api_key() -> bool:
    """Check if a real API key is configured for the current provider."""
    provider = os.environ.get("LLM_PROVIDER", "anthropic")
    if provider == "ollama":
        return True
    if provider == "anthropic":
        return bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())
    if provider == "github":
        return bool(os.environ.get("GITHUB_TOKEN", "").strip())
    if provider == "openai":
        return bool(os.environ.get("OPENAI_API_KEY", "").strip())
    return False


@app.get("/api/settings/llm")
async def get_llm_settings(state: AppState = Depends(get_state)):
    """Return current LLM configuration (never exposes actual API key)."""
    provider = os.environ.get("LLM_PROVIDER", "anthropic")

    has_key = _has_llm_api_key()
    connected = state.llm_client is not None and has_key

    return {
        "provider": provider,
        "model": state.model or "",
        "base_url": os.environ.get(
            {
                "ollama": "OLLAMA_BASE_URL",
                "github": "GITHUB_MODELS_BASE_URL",
                "openai": "OPENAI_BASE_URL",
            }.get(provider, "ANTHROPIC_BASE_URL"),
            "",
        )
        or "",
        "has_api_key": has_key,
        "connected": connected,
        # Per-provider env availability so UI can show "Set from environment"
        "env_keys": {
            "anthropic": bool(os.environ.get("ANTHROPIC_API_KEY", "").strip()),
            "github": bool(os.environ.get("GITHUB_TOKEN", "").strip()),
            "ollama": True,
            "openai": bool(os.environ.get("OPENAI_API_KEY", "").strip()),
        },
        "env_models": {
            "anthropic": os.environ.get("ANTHROPIC_MODEL", ""),
            "github": os.environ.get("GITHUB_MODELS_MODEL", ""),
            "ollama": os.environ.get("OLLAMA_MODEL", ""),
            "openai": os.environ.get("OPENAI_MODEL", ""),
        },
    }


@app.get("/api/project-health")
async def get_project_health(state: AppState = Depends(get_state)):
    """Return lightweight project health diagnostics for the UI."""
    health, cached = await _get_cached_insight(
        state,
        "project-health",
        lambda: _build_project_health(state),
    )
    return {**health, "cached": cached}


@app.post("/api/settings/llm")
async def update_llm_settings(request: Request, state: AppState = Depends(get_state)):
    """Update LLM configuration and reinitialize the client.

    Request body:
        provider: "anthropic" | "ollama" | "github" | "openai"
        api_key: API key (optional for ollama)
        model: model name
        base_url: custom base URL (optional)
    """
    body = await request.json()
    provider = body.get("provider", "anthropic")
    api_key = body.get("api_key", "")
    model = body.get("model", "")
    base_url = body.get("base_url", "")

    async with state.state_lock:
        # Set environment variables for the chosen provider
        os.environ["LLM_PROVIDER"] = provider

        if provider == "anthropic":
            if api_key:
                os.environ["ANTHROPIC_API_KEY"] = api_key
            if model:
                os.environ["ANTHROPIC_MODEL"] = model
            if base_url:
                os.environ["ANTHROPIC_BASE_URL"] = base_url
        elif provider == "ollama":
            if model:
                os.environ["OLLAMA_MODEL"] = model
            if base_url:
                os.environ["OLLAMA_BASE_URL"] = base_url
        elif provider == "github":
            if api_key:
                os.environ["GITHUB_TOKEN"] = api_key
            if model:
                os.environ["GITHUB_MODELS_MODEL"] = model
            if base_url:
                os.environ["GITHUB_MODELS_BASE_URL"] = base_url
        elif provider == "openai":
            if api_key:
                os.environ["OPENAI_API_KEY"] = api_key
            if model:
                os.environ["OPENAI_MODEL"] = model
            if base_url:
                os.environ["OPENAI_BASE_URL"] = base_url

        # Reinitialize LLM client
        init_llm_client(state)
        state.clear_insight_cache()

        # Rebuild system prompt if schema is loaded
        if state.schema_text:
            state.rebuild_system_prompt()

        connected = state.llm_client is not None

    # Validate the connection with a lightweight test request
    error_msg = ""
    if connected and state.llm_client is not None:
        try:
            await state.llm_client.create_message(
                model=state.model,
                max_tokens=1,
                system="Reply with OK.",
                messages=[{"role": "user", "content": "ping"}],
                tools=[],
            )
            logger.info(f"LLM configured: provider={provider}, model={state.model}")
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"LLM validation failed for provider={provider}: {e}")
            connected = False
    else:
        logger.warning(f"LLM configuration failed for provider={provider}")

    return {
        "connected": connected,
        "provider": provider,
        "model": state.model or "",
        "has_api_key": _has_llm_api_key(),
        "base_url": "",
        "error": error_msg if not connected else "",
        "env_keys": {
            "anthropic": bool(os.environ.get("ANTHROPIC_API_KEY", "").strip()),
            "github": bool(os.environ.get("GITHUB_TOKEN", "").strip()),
            "ollama": True,
            "openai": bool(os.environ.get("OPENAI_API_KEY", "").strip()),
        },
        "env_models": {
            "anthropic": os.environ.get("ANTHROPIC_MODEL", ""),
            "github": os.environ.get("GITHUB_MODELS_MODEL", ""),
            "ollama": os.environ.get("OLLAMA_MODEL", ""),
            "openai": os.environ.get("OPENAI_MODEL", ""),
        },
    }


@app.post("/api/sql-confirm/{request_id}")
async def sql_confirm(request_id: str, request: Request, state: AppState = Depends(get_state)):
    """Approve, edit, or reject a pending SQL confirmation."""
    body = await request.json()
    action = body.get("action", "reject")
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
async def clear_conversations(state: AppState = Depends(get_state)):
    """Remove all conversations."""
    if state.conversations is None:
        return {"ok": True}
    state.conversations.clear_all()
    return {"ok": True}


@app.get("/api/conversations")
async def list_conversations(state: AppState = Depends(get_state)):
    """Return a list of all conversations with titles."""
    if state.conversations is None:
        return {"conversations": []}
    return {"conversations": list(reversed(state.conversations.list_all()))}


@app.get("/api/conversations/{session_id}")
async def get_conversation(session_id: str, state: AppState = Depends(get_state)):
    """Return the event log for a conversation (for replay)."""
    if state.conversations is None:
        return SafeJSONResponse(
            {"events": [], "title": "Untitled", "dashboard": _empty_dashboard()}
        )
    data = state.conversations.get(session_id)
    dashboard = data.get("dashboard")
    if not isinstance(dashboard, dict):
        dashboard = _empty_dashboard()
    return SafeJSONResponse(
        {
            "events": data["events"],
            "title": data.get("title", "Untitled"),
            "dashboard": dashboard,
        }
    )


@app.get("/api/conversations/{session_id}/events/{event_index}/plotly-spec")
async def get_conversation_plotly_spec(
    session_id: str,
    event_index: int,
    state: AppState = Depends(get_state),
):
    """Return the Plotly spec for a persisted chart event."""
    validate_session_id(session_id)
    if state.conversations is None:
        return SafeJSONResponse({"plotly_spec": None})
    data = state.conversations.get(session_id)
    events = data.get("events", [])
    if event_index < 0 or event_index >= len(events):
        return SafeJSONResponse({"plotly_spec": None})
    event = events[event_index]
    if event.get("event") != EventType.TOOL_RESULT:
        return SafeJSONResponse({"plotly_spec": None})
    event_data = event.get("data") or {}
    return SafeJSONResponse(
        {
            "plotly_spec": event_data.get("plotly_spec") or event_data.get("plotlySpec"),
        }
    )


@app.post("/api/chat")
async def chat(request: Request, state: AppState = Depends(get_state)):
    """Handle chat messages with SSE streaming."""
    body = await request.json()
    message = body.get("message", "").strip()
    session_id = body.get("session_id", "default")
    truncate_raw = body.get("truncate_before_turn")
    truncate_before_turn: int | None = None
    if truncate_raw is not None:
        if not isinstance(truncate_raw, int) or isinstance(truncate_raw, bool) or truncate_raw < 0:
            return StreamingResponse(
                iter(
                    [
                        f'event: {EventType.ERROR}\ndata: {{"error":"Invalid truncate_before_turn"}}\n\n'
                    ]
                ),
                media_type="text/event-stream",
            )
        truncate_before_turn = truncate_raw

    try:
        validate_session_id(session_id)
    except InvalidSessionIdError:
        return StreamingResponse(
            iter([f'event: {EventType.ERROR}\ndata: {{"error":"Invalid session ID"}}\n\n']),
            media_type="text/event-stream",
        )

    if not message:
        return StreamingResponse(
            iter(["event: done\ndata: {}\n\n"]),
            media_type="text/event-stream",
        )

    session_lock = state.get_session_lock(session_id)

    async def locked_generator() -> AsyncIterator[str]:
        async with session_lock:
            try:
                async for event in generate_chat_response(
                    message,
                    session_id,
                    state,
                    request=request,
                    truncate_before_turn=truncate_before_turn,
                ):
                    yield event
            except Exception as exc:
                logger.exception("Chat stream failed")
                yield f"event: {EventType.ERROR}\ndata: {json.dumps({'error': str(exc)})}\n\n"
                yield "event: done\ndata: {}\n\n"

    return StreamingResponse(
        locked_generator(),
        media_type="text/event-stream",
    )


@app.post("/api/clear")
async def clear_session(request: Request, state: AppState = Depends(get_state)):
    """Clear a chat session."""
    body = await request.json()
    session_id = body.get("session_id", "default")
    if state.conversations is None:
        return {"ok": True}
    state.conversations.delete(session_id)
    return {"ok": True}


@app.post("/api/export/{session_id}")
async def export_session(session_id: str, request: Request, state: AppState = Depends(get_state)):
    """Export a conversation as HTML, Python, or a bundle zip."""
    from datasight.export import (
        export_session_bundle,
        export_session_html,
        export_session_python,
        normalize_bundle_includes,
    )

    try:
        validate_session_id(session_id)
    except InvalidSessionIdError:
        return PlainTextResponse(content="Invalid session ID", status_code=400)

    body = await request.json()
    exclude = body.get("exclude_indices", [])
    exclude_set = set(exclude) if exclude else None
    fmt = (body.get("format") or "html").lower()
    include = body.get("include")

    if state.conversations is None:
        if fmt == "html":
            return HTMLResponse(content="<p>No conversation data available.</p>", status_code=200)
        return PlainTextResponse(content="No conversation data available.", status_code=404)

    data = state.conversations.get(session_id)
    events = data.get("events", [])
    title = data.get("title", "datasight session")

    if fmt == "py":
        db_path, db_mode = _resolve_export_db_target(state)
        script = export_session_python(
            events,
            title=title,
            db_path=db_path,
            db_mode=db_mode,
            exclude_indices=exclude_set,
        )
        return PlainTextResponse(
            content=script,
            media_type="text/x-python",
            headers={
                "Content-Disposition": 'attachment; filename="datasight-session.py"',
            },
        )

    if fmt == "bundle":
        db_path, db_mode = _resolve_export_db_target(state)
        try:
            include_values = normalize_bundle_includes(include)
            bundle = export_session_bundle(
                events,
                title=title,
                session_id=session_id,
                db_path=db_path,
                db_mode=db_mode,
                exclude_indices=exclude_set,
                include=include_values,
            )
        except ValueError as exc:
            return PlainTextResponse(content=str(exc), status_code=400)
        return StreamingResponse(
            iter([bundle]),
            media_type="application/zip",
            headers={
                "Content-Disposition": 'attachment; filename="datasight-bundle.zip"',
            },
        )

    html = export_session_html(events, title=title, exclude_indices=exclude_set)
    return HTMLResponse(
        content=html,
        headers={
            "Content-Disposition": 'attachment; filename="datasight-export.html"',
        },
    )


def _resolve_export_db_target(state: AppState) -> tuple[str, str]:
    """Resolve (db_path, db_mode) for embedding into an exported Python script.

    Returns absolute paths for file-backed databases. For non-file backends or
    when no project is loaded, returns ("", db_mode) so the script renders a
    TODO scaffold instead of a hardcoded connection.
    """
    from datasight.cli import _resolve_db_path, _resolve_settings

    if not state.project_dir:
        return "", state.sql_dialect or "duckdb"
    try:
        settings, _ = _resolve_settings(state.project_dir)
    except Exception:
        return "", state.sql_dialect or "duckdb"
    db_mode = settings.database.mode or "duckdb"
    db_path = _resolve_db_path(settings, state.project_dir)
    return db_path, db_mode


@app.post("/api/dashboard/export")
async def export_dashboard(request: Request):
    """Export dashboard as self-contained HTML."""
    from datasight.export import export_dashboard_html

    body = await request.json()
    items = body.get("items", [])
    title = body.get("title", "")
    columns = body.get("columns", 2)
    filters = body.get("filters", [])

    html = export_dashboard_html(items, title=title, columns=columns, filters=filters)
    return HTMLResponse(
        content=html,
        headers={
            "Content-Disposition": 'attachment; filename="datasight-dashboard.html"',
        },
    )
