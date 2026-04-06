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
import os
import re
import traceback
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Any, AsyncIterator

import pandas as pd
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger

from datasight.agent import df_to_html_table, execute_tool, extract_suggestions
from datasight.config import (
    create_sql_runner_from_settings,
    format_example_queries,
    load_example_queries,
    load_schema_description,
)
from datasight.events import EventType
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
from datasight.runner import SqlRunner
from datasight.schema import format_schema_context, introspect_schema
from datasight.settings import Settings, capture_original_env, restore_original_env
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


class DashboardStore:
    """Persist dashboard items as a JSON file."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._items: list[dict[str, Any]] = []
        self._columns: int = 0
        self._next_id = 1
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text())
                self._items = data.get("items", [])
                self._columns = data.get("columns", 0)
                if self._items:
                    self._next_id = max(item.get("id", 0) for item in self._items) + 1
            except (json.JSONDecodeError, OSError):
                pass

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps({"items": self._items, "columns": self._columns}, indent=2)
        )

    def get_all(self) -> dict[str, Any]:
        return {"items": list(self._items), "columns": self._columns}

    def save_all(self, items: list[dict[str, Any]], columns: int | None = None) -> dict[str, Any]:
        for item in items:
            if "id" not in item:
                item["id"] = self._next_id
                self._next_id += 1
        self._items = items
        if columns is not None:
            self._columns = columns
        if self._items:
            self._next_id = max(item.get("id", 0) for item in self._items) + 1
        self._save()
        return self.get_all()

    def clear(self) -> None:
        self._items = []
        self._columns = 0
        self._next_id = 1
        self._save()


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
        self.conversations: ConversationStore | None = None
        self.bookmarks: BookmarkStore | None = None
        self.dashboard: DashboardStore | None = None
        self.schema_info: list[dict[str, Any]] = []
        self.example_queries_list: list[dict[str, str]] = []
        self.query_logger: QueryLogger | None = None
        self.confirm_sql: bool = False
        self.explain_sql: bool = False
        self.clarify_sql: bool = True
        self.schema_text: str = ""
        self.schema_map: dict[str, set[str]] = {}
        self.sql_dialect: str = "duckdb"
        self.project_dir: str | None = None
        self.project_loaded: bool = False
        self.pending_confirms: dict[str, dict[str, Any]] = {}
        self._response_cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._response_cache_max = 100
        self._max_history_pairs = 10

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
        self.system_prompt = ""
        self.project_dir = None
        self.project_loaded = False
        # Reset LLM state to force reinitialization on next project load
        self.llm_client = None
        self.model = ""
        self.conversations = None
        self.bookmarks = None
        self.dashboard = None
        self.query_logger = None
        self._response_cache.clear()

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

    def get_session_messages(self, session_id: str) -> list[dict[str, Any]]:
        """Get messages for a session."""
        if self.conversations is None:
            raise ConfigurationError("App not initialized")
        return self.conversations.get(session_id)["messages"]

    def trim_messages(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Keep only recent messages to bound input token growth."""
        exchange_starts: list[int] = []
        for i, msg in enumerate(messages):
            if msg["role"] == "user" and isinstance(msg["content"], str):
                exchange_starts.append(i)

        if len(exchange_starts) <= self._max_history_pairs:
            return messages

        cut = exchange_starts[-self._max_history_pairs]
        return messages[cut:]


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
        )
        # Only update state after successful creation
        state.llm_client = client
        state.model = settings.llm.model
    except LLMError as e:
        logger.warning(f"Failed to initialize LLM client: {e}")
        # Clear client on failure to prevent stale state
        state.llm_client = None
        state.model = ""


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
        state.sql_runner = create_sql_runner_from_settings(settings.database, project_dir)
    except Exception as e:
        logger.error(f"Failed to create SQL runner:\n{traceback.format_exc()}")
        raise ProjectError(f"Failed to connect to database: {e}") from e

    state.project_dir = project_dir
    add_recent_project(project_dir)

    # Set up project-specific storage
    datasight_dir = Path(project_dir) / ".datasight"
    state.conversations = ConversationStore(datasight_dir / "conversations")
    state.bookmarks = BookmarkStore(datasight_dir / "bookmarks.json")
    state.dashboard = DashboardStore(datasight_dir / "dashboard.json")

    # Load settings
    state.confirm_sql = settings.app.confirm_sql
    state.explain_sql = settings.app.explain_sql
    state.clarify_sql = settings.app.clarify_sql
    state._max_history_pairs = settings.app.max_history_pairs
    state._response_cache_max = settings.app.response_cache_max

    log_path = os.environ.get("QUERY_LOG_PATH", os.path.join(project_dir, "query_log.jsonl"))
    state.query_logger = QueryLogger(path=log_path, enabled=settings.app.log_queries)

    # Load schema and introspect database
    user_desc = load_schema_description(os.environ.get("SCHEMA_DESCRIPTION_PATH"), project_dir)

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
    }


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------


async def _startup() -> None:
    """Initialize the LLM client on startup."""
    from dotenv import load_dotenv

    load_dotenv()

    # Capture env after root .env is loaded as baseline for project switching
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

    port = os.environ.get("PORT", "8084")
    if _state.project_loaded:
        logger.info(f"datasight ready (model={_state.model}, project={_state.project_dir})")
    else:
        logger.info(f"datasight ready (model={_state.model}, no project loaded)")
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
) -> tuple[str, str | None, str | None, dict[str, Any]]:
    """Execute a tool call via the shared agent module.

    Returns (result_text_for_llm, optional_html_for_ui, optional_chart_html, meta).
    """
    if state.sql_runner is None:
        raise ConfigurationError("SQL runner not initialized")

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


# ---------------------------------------------------------------------------
# Cost Logging
# ---------------------------------------------------------------------------


_MODEL_PRICING: dict[str, tuple[float, float]] = {
    "claude-sonnet-4-20250514": (3.0, 15.0),
    "claude-haiku-4-5-20251001": (0.80, 4.0),
    "claude-opus-4-20250514": (15.0, 75.0),
}


def log_query_cost(model: str, api_calls: int, input_tokens: int, output_tokens: int) -> None:
    """Log token usage and estimated cost."""
    pricing = _MODEL_PRICING.get(model)
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


# ---------------------------------------------------------------------------
# Chat Generator
# ---------------------------------------------------------------------------


async def generate_chat_response(
    message: str,
    session_id: str,
    state: AppState,
) -> AsyncIterator[str]:
    """Generate SSE events for a chat message."""
    if state.conversations is None or state.llm_client is None:
        raise ConfigurationError("App not initialized")

    messages = state.get_session_messages(session_id)
    messages.append({"role": "user", "content": message})

    conv = state.conversations.get(session_id)
    evt_log = conv["events"]
    evt_log.append({"event": EventType.USER_MESSAGE, "data": {"text": message}})

    if conv["title"] == "Untitled":
        conv["title"] = message[:80] + ("..." if len(message) > 80 else "")
    state.conversations.save(session_id)

    is_first_turn = len(messages) == 1
    max_iterations = 15

    # Check cache for first-turn questions
    if is_first_turn:
        cached = state.cache_get(message)
        if cached is not None:
            logger.info(f"[cache] HIT for question: {message[:60]}")
            for evt in cached["events"]:
                evt_log.append(evt)
                yield f"event: {evt['event']}\ndata: {json.dumps(evt['data'])}\n\n"
            for msg in cached["messages"]:
                messages.append(msg)
            state.conversations.save(session_id)
            yield "event: done\ndata: {}\n\n"
            if cached.get("suggestions"):
                evt_log.append(
                    {
                        "event": EventType.SUGGESTIONS,
                        "data": {"suggestions": cached["suggestions"]},
                    }
                )
                state.conversations.save(session_id)
                yield f"event: {EventType.SUGGESTIONS}\ndata: {json.dumps({'suggestions': cached['suggestions']})}\n\n"
            return

    _evt_start = len(evt_log)
    _msg_start = len(messages)

    total_input_tokens = 0
    total_output_tokens = 0
    api_calls = 0

    for _ in range(max_iterations):
        trimmed = state.trim_messages(messages)
        try:
            response = await state.llm_client.create_message(
                model=state.model,
                max_tokens=4096,
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
            state.conversations.save(session_id)
            yield f"event: token\ndata: {json.dumps({'text': f'Error: {e}'})}\n\n"
            yield "event: done\ndata: {}\n\n"
            return
        except Exception as e:
            logger.error(f"Unexpected LLM error:\n{traceback.format_exc()}")
            evt_log.append(
                {
                    "event": EventType.ASSISTANT_MESSAGE,
                    "data": {"text": f"Error: {e}"},
                }
            )
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
            f"cache_read={response.usage.cache_read_input_tokens}"
        )

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
            for block in response.content:
                if not isinstance(block, ToolUseBlock):
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

                result_text, result_html, _, meta = await execute_tool_web(
                    block.name,
                    tool_input,
                    state,
                    session_id=session_id,
                    user_question=message,
                )

                if result_html:
                    is_chart = block.name == "visualize_data" and "<script" in (result_html or "")
                    result_title = tool_input.get("title", message) if is_chart else message
                    tr_data = {
                        "html": result_html,
                        "type": "chart" if is_chart else "table",
                        "title": result_title,
                    }
                    evt_log.append({"event": EventType.TOOL_RESULT, "data": tr_data})
                    yield f"event: {EventType.TOOL_RESULT}\ndata: {json.dumps(tr_data)}\n\n"

                if meta:
                    evt_log.append({"event": EventType.TOOL_DONE, "data": meta})
                    yield f"event: {EventType.TOOL_DONE}\ndata: {json.dumps(meta)}\n\n"
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

        # Final text response
        text = "".join(b.text for b in response.content if isinstance(b, TextBlock))
        suggestions = extract_suggestions(text)
        if suggestions:
            text = re.split(r"\n---\s*\n", text, maxsplit=1)[0].rstrip()

        messages.append({"role": "assistant", "content": text})
        evt_log.append({"event": EventType.ASSISTANT_MESSAGE, "data": {"text": text}})
        state.conversations.save(session_id)

        for i, word in enumerate(text.split(" ")):
            chunk = word if i == 0 else " " + word
            yield f"event: token\ndata: {json.dumps({'text': chunk})}\n\n"
            await asyncio.sleep(0.015)

        log_query_cost(state.model, api_calls, total_input_tokens, total_output_tokens)

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
                },
            )
            logger.info(f"[cache] STORED for question: {message[:60]}")

        yield "event: done\ndata: {}\n\n"

        if suggestions:
            evt_log.append(
                {
                    "event": EventType.SUGGESTIONS,
                    "data": {"suggestions": suggestions},
                }
            )
            state.conversations.save(session_id)
            yield f"event: {EventType.SUGGESTIONS}\ndata: {json.dumps({'suggestions': suggestions})}\n\n"
        return

    log_query_cost(state.model, api_calls, total_input_tokens, total_output_tokens)
    max_iter_text = "Reached maximum number of tool calls. Please try a simpler question."
    evt_log.append({"event": EventType.ASSISTANT_MESSAGE, "data": {"text": max_iter_text}})
    state.conversations.save(session_id)
    yield f"event: token\ndata: {json.dumps({'text': max_iter_text})}\n\n"
    yield "event: done\ndata: {}\n\n"


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


@app.get("/api/queries")
async def get_queries(state: AppState = Depends(get_state)):
    """Return example queries."""
    return {"queries": state.example_queries_list}


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
            logger.error(f"Summarize error:\n{traceback.format_exc()}")
            yield f"event: {EventType.ERROR}\ndata: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/project")
async def get_current_project(state: AppState = Depends(get_state)):
    """Return current project info, or null if no project is loaded."""
    if not state.project_loaded or state.project_dir is None:
        return {"loaded": False, "path": None, "name": None}
    return {
        "loaded": True,
        "path": state.project_dir,
        "name": get_project_name(state.project_dir),
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

    try:
        result = await load_project(project_path, state)
        return {"success": True, **result}
    except ProjectError as e:
        logger.error(f"Failed to load project {project_path}: {e}")
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error loading project:\n{traceback.format_exc()}")
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


@app.get("/api/preview/{table_name}")
async def preview_table(table_name: str, state: AppState = Depends(get_state)):
    """Return an HTML table preview of the first 10 rows."""
    valid_names = {t["name"] for t in state.schema_info}
    if table_name not in valid_names:
        return {"html": None, "error": "Unknown table"}
    if state.sql_runner is None:
        return {"html": None, "error": "Database not connected"}
    try:
        df = await state.sql_runner.run_sql(f'SELECT * FROM "{table_name}" LIMIT 10')
        html = df_to_html_table(df, max_rows=10)
        return {"html": html}
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
        return {"stats": stats}
    except Exception as e:
        logger.debug(f"Column stats error: {e}")
        return {"stats": None, "error": str(e)}


@app.post("/api/query-log/toggle")
async def toggle_query_log(state: AppState = Depends(get_state)):
    """Enable or disable query logging at runtime."""
    if state.query_logger is None:
        return {"enabled": False}
    state.query_logger.enabled = not state.query_logger.enabled
    logger.info(f"Query logging {'enabled' if state.query_logger.enabled else 'disabled'}")
    return {"enabled": state.query_logger.enabled}


@app.get("/api/query-log")
async def get_query_log(n: int = 50, state: AppState = Depends(get_state)):
    """Return recent query log entries."""
    if state.query_logger is None:
        return {"entries": [], "enabled": False}
    return {"entries": state.query_logger.read_recent(n), "enabled": state.query_logger.enabled}


@app.get("/api/bookmarks")
async def list_bookmarks(state: AppState = Depends(get_state)):
    """Return all bookmarked queries."""
    if state.bookmarks is None:
        raise ConfigurationError("App not initialized")
    return {"bookmarks": state.bookmarks.list_all()}


@app.post("/api/bookmarks")
async def add_bookmark(request: Request, state: AppState = Depends(get_state)):
    """Add a bookmarked query."""
    if state.bookmarks is None:
        raise ConfigurationError("App not initialized")
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
        raise ConfigurationError("App not initialized")
    state.bookmarks.delete(bookmark_id)
    return {"ok": True}


@app.delete("/api/bookmarks")
async def clear_bookmarks(state: AppState = Depends(get_state)):
    """Remove all bookmarks."""
    if state.bookmarks is None:
        raise ConfigurationError("App not initialized")
    state.bookmarks.clear()
    return {"ok": True}


@app.get("/api/dashboard")
async def get_dashboard(state: AppState = Depends(get_state)):
    """Return all dashboard items and layout settings."""
    if state.dashboard is None:
        return {"items": [], "columns": 0}
    return state.dashboard.get_all()


@app.post("/api/dashboard")
async def save_dashboard(request: Request, state: AppState = Depends(get_state)):
    """Save dashboard items and layout settings."""
    if state.dashboard is None:
        raise ConfigurationError("App not initialized")
    body = await request.json()
    items = body.get("items", [])
    columns = body.get("columns")
    result = state.dashboard.save_all(items, columns)
    return result


@app.delete("/api/dashboard")
async def clear_dashboard(state: AppState = Depends(get_state)):
    """Clear all dashboard items."""
    if state.dashboard is None:
        raise ConfigurationError("App not initialized")
    state.dashboard.clear()
    return {"ok": True}


@app.get("/api/settings")
async def get_settings(state: AppState = Depends(get_state)):
    """Return current feature toggles."""
    return {
        "confirm_sql": state.confirm_sql,
        "explain_sql": state.explain_sql,
        "clarify_sql": state.clarify_sql,
    }


@app.post("/api/settings")
async def update_settings(request: Request, state: AppState = Depends(get_state)):
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
        raise ConfigurationError("App not initialized")
    state.conversations.clear_all()
    return {"ok": True}


@app.get("/api/conversations")
async def list_conversations(state: AppState = Depends(get_state)):
    """Return a list of all conversations with titles."""
    if state.conversations is None:
        raise ConfigurationError("App not initialized")
    return {"conversations": list(reversed(state.conversations.list_all()))}


@app.get("/api/conversations/{session_id}")
async def get_conversation(session_id: str, state: AppState = Depends(get_state)):
    """Return the event log for a conversation (for replay)."""
    if state.conversations is None:
        raise ConfigurationError("App not initialized")
    data = state.conversations.get(session_id)
    return {"events": data["events"], "title": data.get("title", "Untitled")}


@app.post("/api/chat")
async def chat(request: Request, state: AppState = Depends(get_state)):
    """Handle chat messages with SSE streaming."""
    body = await request.json()
    message = body.get("message", "").strip()
    session_id = body.get("session_id", "default")

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

    return StreamingResponse(
        generate_chat_response(message, session_id, state),
        media_type="text/event-stream",
    )


@app.post("/api/clear")
async def clear_session(request: Request, state: AppState = Depends(get_state)):
    """Clear a chat session."""
    body = await request.json()
    session_id = body.get("session_id", "default")
    if state.conversations is None:
        raise ConfigurationError("App not initialized")
    state.conversations.delete(session_id)
    return {"ok": True}


@app.post("/api/export/{session_id}")
async def export_session(session_id: str, request: Request, state: AppState = Depends(get_state)):
    """Export a conversation as self-contained HTML."""
    from datasight.export import export_session_html

    if state.conversations is None:
        raise ConfigurationError("App not initialized")
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


@app.post("/api/dashboard/export")
async def export_dashboard(request: Request):
    """Export dashboard as self-contained HTML."""
    from datasight.export import export_dashboard_html

    body = await request.json()
    items = body.get("items", [])
    title = body.get("title", "datasight dashboard")
    columns = body.get("columns", 2)

    html = export_dashboard_html(items, title=title, columns=columns)
    return HTMLResponse(
        content=html,
        headers={
            "Content-Disposition": 'attachment; filename="datasight-dashboard.html"',
        },
    )
