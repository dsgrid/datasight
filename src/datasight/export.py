"""
Export a datasight conversation session as a self-contained HTML page.

Renders user messages, assistant responses, SQL results (tables), and
Plotly charts into a single shareable HTML file.
"""

from __future__ import annotations

import json
import re
from typing import Any

from datasight.chart import build_chart_html
from datasight.events import EventType
from datasight.templating import escape_html_attr, json_for_script, render_template

PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.2.min.js"
MARKED_CDN = "https://cdn.jsdelivr.net/npm/marked/marked.min.js"
HLJS_CDN = "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"
HLJS_SQL_CDN = "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/sql.min.js"
HLJS_CSS = "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github.min.css"
DOMPURIFY_CDN = "https://cdn.jsdelivr.net/npm/dompurify@3/dist/purify.min.js"


def _format_provenance_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Format provenance tool entries for Mustache rendering."""
    formatted: list[dict[str, Any]] = []
    for tool in tools:
        validation = tool.get("validation") or {}
        execution = tool.get("execution") or {}
        columns = execution.get("columns") or []
        formatted.append(
            {
                "tool": tool.get("tool") or "tool",
                "sql": tool.get("formatted_sql") or tool.get("sql") or "",
                "validation_status": validation.get("status") or "not_run",
                "validation_errors": validation.get("errors") or [],
                "execution_status": execution.get("status") or "unknown",
                "execution_time_ms": execution.get("execution_time_ms"),
                "row_count": execution.get("row_count"),
                "column_count": execution.get("column_count"),
                "columns": ", ".join(str(column) for column in columns),
                "error": execution.get("error") or "",
            }
        )
    return formatted


def _group_events_into_blocks(
    events: list[dict[str, Any]],
    exclude_indices: set[int],
) -> list[dict[str, Any]]:
    """Group events into message blocks for rendering.

    Returns a list of block dicts with type flags for Mustache conditionals.
    Note: We pass raw text to the template and let Mustache handle escaping
    via {{}} syntax. For pre-escaped content (like HTML tables), we use
    {{{...}}} in the template.

    Exclusion is turn-based: excluding a user message index excludes the
    entire turn (user message, tools, and assistant reply). This matches
    the frontend's semantic grouping.
    """
    blocks: list[dict[str, Any]] = []
    current_user_idx: int | None = None  # Index of current turn's user message
    user_count = 0  # Counter for user messages (used as turn index)
    pending_events: list[dict[str, Any]] = []

    def flush_pending():
        nonlocal pending_events
        if pending_events:
            for evt in pending_events:
                blocks.append(evt)
            pending_events = []

    def is_turn_excluded() -> bool:
        """Check if the current turn should be excluded."""
        return current_user_idx is not None and current_user_idx in exclude_indices

    for evt in events:
        etype = evt.get("event")
        data = evt.get("data", {})

        match etype:
            case EventType.USER_MESSAGE:
                flush_pending()
                current_user_idx = user_count
                if not is_turn_excluded():
                    # Let Mustache handle escaping via {{text}}
                    blocks.append(
                        {
                            "is_user": True,
                            "text": data.get("text", ""),
                        }
                    )
                user_count += 1

            case EventType.ASSISTANT_MESSAGE:
                # Assistant is part of the current turn
                if not is_turn_excluded():
                    # Escape for HTML attribute (data-md="...")
                    pending_events.append(
                        {
                            "is_assistant": True,
                            "text": escape_html_attr(data.get("text", "")),
                        }
                    )
                flush_pending()

            case EventType.TOOL_START:
                sql = data.get("input", {}).get("sql", "")
                # Tools are part of the current turn
                if sql and not is_turn_excluded():
                    # Let Mustache handle escaping via {{sql}}
                    pending_events.append(
                        {
                            "is_sql": True,
                            "sql": sql,
                        }
                    )

            case EventType.TOOL_RESULT:
                html = data.get("html", "")
                rtype = data.get("type", "table")
                # Tools are part of the current turn
                if not is_turn_excluded():
                    if rtype == "chart":
                        spec = data.get("plotly_spec") or data.get("plotlySpec")
                        chart_html = html
                        if isinstance(spec, dict):
                            chart_html = build_chart_html(
                                spec,
                                str(data.get("title") or "Chart"),
                            )
                        if chart_html:
                            pending_events.append(
                                {
                                    "is_chart": True,
                                    "html": escape_html_attr(chart_html),
                                }
                            )
                    else:
                        if html:
                            pending_events.append(
                                {
                                    "is_table": True,
                                    "html": html,  # Already escaped in df_to_html_table
                                }
                            )

            case EventType.PROVENANCE:
                if not is_turn_excluded():
                    llm = data.get("llm") or {}
                    pending_events.append(
                        {
                            "is_provenance": True,
                            "model": data.get("model", ""),
                            "dialect": data.get("dialect", ""),
                            "project_dir": data.get("project_dir", ""),
                            "api_calls": llm.get("api_calls"),
                            "input_tokens": llm.get("input_tokens"),
                            "output_tokens": llm.get("output_tokens"),
                            "estimated_cost": llm.get("estimated_cost"),
                            "warnings": data.get("warnings") or [],
                            "tools": _format_provenance_tools(data.get("tools") or []),
                        }
                    )

    flush_pending()
    return blocks


def export_session_html(
    events: list[dict[str, Any]],
    title: str = "datasight session",
    exclude_indices: set[int] | None = None,
) -> str:
    """Render conversation events as a self-contained HTML page.

    Parameters
    ----------
    events:
        List of event dicts from a conversation (same format as stored
        in ``.datasight/conversations/<id>.json``).
    title:
        Page title.
    exclude_indices:
        Optional set of turn indices (0-based, where each turn is a
        user question plus its tools and assistant response) to exclude
        from the export.

    Returns
    -------
    A complete HTML string.
    """
    if exclude_indices is None:
        exclude_indices = set()

    blocks = _group_events_into_blocks(events, exclude_indices)

    return render_template(
        "export_session",
        {
            "title": title,  # Let Mustache escape via {{title}}
            "hljs_css": HLJS_CSS,
            "hljs_cdn": HLJS_CDN,
            "hljs_sql_cdn": HLJS_SQL_CDN,
            "marked_cdn": MARKED_CDN,
            "dompurify_cdn": DOMPURIFY_CDN,
            "blocks": blocks,
        },
    )


def _shorten_for_header(text: str, max_len: int = 80) -> str:
    """Collapse whitespace and clip to one short line for a section header."""
    collapsed = " ".join((text or "").split())
    if len(collapsed) <= max_len:
        return collapsed
    return collapsed[: max_len - 1] + "…"


def _wrap_as_comments(text: str, prefix: str = "# ") -> list[str]:
    """Wrap multi-line text as Python comment lines (no line wrapping)."""
    if not text:
        return []
    return [f"{prefix}{line}" if line else "#" for line in text.splitlines()]


def _group_events_into_turns(
    events: list[dict[str, Any]],
    exclude_indices: set[int],
) -> list[dict[str, Any]]:
    """Group an evt_log into turns keyed by user message ordinal.

    Each returned turn is::

        {
            "index": int,                 # 0-based turn index
            "question": str,
            "intro_texts": list[str],     # assistant text blocks before tools
            "final_texts": list[str],     # assistant text blocks after tools
            "tool_calls": list[ToolCall], # SQL / chart calls in order
        }

    The agent can emit multiple ``ASSISTANT_MESSAGE`` events per phase (one per
    text block streamed before tools, plus the final answer), so we accumulate
    them as lists and let the renderer join them — overwriting would silently
    drop narrative text.

    A ToolCall is::

        {
            "tool_name": str,
            "sql": str | None,
            "plotly_spec": dict | None,
            "chart_title": str | None,
            "error": str | None,
        }
    """
    turns: list[dict[str, Any]] = []
    user_count = -1
    current: dict[str, Any] | None = None
    pending_start: dict[str, Any] | None = None
    pending_result: dict[str, Any] | None = None

    for evt in events:
        etype = evt.get("event")
        data = evt.get("data") or {}

        if etype == EventType.USER_MESSAGE:
            if current is not None:
                turns.append(current)
            user_count += 1
            if user_count in exclude_indices:
                current = None
            else:
                current = {
                    "index": user_count,
                    "question": data.get("text", ""),
                    "intro_texts": [],
                    "final_texts": [],
                    "tool_calls": [],
                }
            pending_start = None
            pending_result = None
            continue

        if current is None:
            continue

        if etype == EventType.ASSISTANT_MESSAGE:
            text = data.get("text", "")
            if not text:
                continue
            # Bucket by phase: text emitted before any tool call is intro
            # narrative; text emitted after is the final answer.
            if not current["tool_calls"]:
                current["intro_texts"].append(text)
            else:
                current["final_texts"].append(text)
        elif etype == EventType.TOOL_START:
            pending_start = data
            pending_result = None
        elif etype == EventType.TOOL_RESULT:
            pending_result = data
        elif etype == EventType.TOOL_DONE:
            tool_name = (pending_start or {}).get("tool") or data.get("tool") or "tool"
            sql = (
                data.get("formatted_sql")
                or data.get("sql")
                or ((pending_start or {}).get("input") or {}).get("sql")
            )
            spec = None
            chart_title = None
            if pending_result:
                spec = pending_result.get("plotly_spec") or pending_result.get("plotlySpec")
                chart_title = pending_result.get("title")
            current["tool_calls"].append(
                {
                    "tool_name": tool_name,
                    "sql": sql,
                    "plotly_spec": spec,
                    "chart_title": chart_title,
                    "error": data.get("error"),
                }
            )
            pending_start = None
            pending_result = None

    if current is not None:
        turns.append(current)
    return turns


def _python_tool_call_context(
    turn_number: int, index_in_turn: int | None, call: dict[str, Any]
) -> dict[str, Any]:
    """Build the Mustache context for one SQL / chart tool call."""
    suffix = f"_{index_in_turn}" if index_in_turn is not None else ""
    name = f"{turn_number}{suffix}"
    spec = call.get("plotly_spec")
    chart_title = call.get("chart_title") or f"turn {turn_number}{suffix}"
    return {
        "name": name,
        "turn_number": turn_number,
        "suffix": suffix,
        "has_error": bool(call.get("error")),
        "error_short": _shorten_for_header(call.get("error") or ""),
        "has_sql": bool(call.get("sql")),
        # Mustache emits the literal SQL between triple-quote fences, so strip
        # any trailing whitespace to keep the closing fence flush.
        "sql_text": (call.get("sql") or "").rstrip(),
        "has_chart": bool(spec),
        # Embed the spec as a JSON string the user can re-edit. Escape any
        # accidental triple-quotes so the surrounding `r"""..."""` stays valid.
        "chart_spec_json": (
            json.dumps(spec, ensure_ascii=False, indent=2).replace('"""', '\\"\\"\\"')
            if spec
            else ""
        ),
        "chart_title_short": _shorten_for_header(chart_title),
    }


def _python_turn_context(turn: dict[str, Any]) -> dict[str, Any] | None:
    """Build the Mustache context for one turn, or None if nothing to render."""
    runnable = [c for c in turn.get("tool_calls", []) if c.get("sql") or c.get("plotly_spec")]
    # Each phase can carry multiple ASSISTANT_MESSAGE events; preserve them all
    # by concatenating with a blank line so paragraph boundaries survive in the
    # rendered comment block.
    intro = _wrap_as_comments("\n\n".join(turn.get("intro_texts") or []))
    final = _wrap_as_comments("\n\n".join(turn.get("final_texts") or []), prefix="# Assistant: ")
    if not runnable and not intro and not final:
        return None
    multi = len(runnable) > 1
    return {
        "turn_number": turn["index"] + 1,
        "header_question": _shorten_for_header(turn.get("question") or ""),
        "intro_comments": intro,
        "tool_calls": [
            _python_tool_call_context(turn["index"] + 1, i if multi else None, call)
            for i, call in enumerate(runnable, start=1)
        ],
        "final_comments": final,
    }


def export_session_python(
    events: list[dict[str, Any]],
    *,
    title: str = "datasight session",
    db_path: str = "",
    db_mode: str = "duckdb",
    exclude_indices: set[int] | None = None,
) -> str:
    """Render conversation events as a runnable, hand-editable Python script.

    Each turn becomes a labelled section with the user question as a comment,
    SQL as a top-of-section ``SQL_N`` constant (the obvious thing to tweak),
    and any Plotly chart embedded as a JSON string fed into ``go.Figure``.
    The output script accepts ``--db`` and ``--output-dir`` so the user can
    move it between machines or redirect chart output without editing.

    Parameters
    ----------
    events:
        Event log from a session (same shape as stored in ``conversations/``).
    title:
        Used in the script's docstring for traceability.
    db_path:
        Default value baked into the script as ``DEFAULT_DB_PATH``. The user
        can override at runtime with ``--db``.
    db_mode:
        ``"duckdb"`` (default), ``"sqlite"``, or another value (renders a
        scaffold whose ``run_sql`` raises NotImplementedError).
    exclude_indices:
        Optional set of 0-based turn indices to skip — same semantics as
        ``export_session_html``.

    Returns
    -------
    The complete script text.
    """
    exclude = exclude_indices or set()
    turns = _group_events_into_turns(events, exclude)
    contexts = [_python_turn_context(t) for t in turns]
    return render_template(
        "export_session_python",
        {
            "title": (title or "datasight session").replace('"""', "'''"),
            "default_db_path_repr": repr(db_path),
            "db_mode": db_mode,
            "is_duckdb": db_mode == "duckdb",
            "is_sqlite": db_mode == "sqlite",
            "is_unknown_mode": db_mode not in ("duckdb", "sqlite"),
            "turns": [c for c in contexts if c is not None],
        },
    )


def _extract_plotly_spec(srcdoc: str) -> dict[str, Any] | None:
    """Extract the Plotly spec JSON from a chart iframe's srcdoc."""
    match = re.search(r"var spec = ({.*?});", srcdoc, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            return None
    return None


def _build_dashboard_cards(
    items: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build card data and chart specs for dashboard export.

    Returns (cards_list, chart_specs_list).
    """
    chart_specs: list[dict[str, Any]] = []
    cards: list[dict[str, Any]] = []

    for idx, item in enumerate(items):
        item_type = item.get("type", "table")
        item_title = item.get("title", "")
        source_meta = item.get("source_meta") or {}

        card: dict[str, Any] = {
            "idx": idx,
            "card_title": item_title if item_title else None,  # Let Mustache escape
        }

        if item_type == "chart":
            # Prefer render_plotly_spec — it has actual data arrays bound in.
            # Otherwise extract a bound spec from a fully-rendered html (the
            # template-driven dashboard apply path ships chart html). Only
            # fall through to plotly_spec last, since it can be the unbound
            # template (e.g. x: "column_name") that Plotly cannot render.
            spec = item.get("render_plotly_spec")
            if not isinstance(spec, dict):
                spec = _extract_plotly_spec(item.get("html", ""))
            if not isinstance(spec, dict):
                candidate = item.get("plotly_spec")
                spec = candidate if isinstance(candidate, dict) else None
            if spec:
                chart_specs.append({"idx": idx, "spec": spec})
                card["is_chart"] = True
            else:
                # Fallback to iframe if no spec is available
                card["is_iframe"] = True
                card["html"] = escape_html_attr(item.get("html", ""))
        elif item_type == "note":
            card["is_note"] = True
            card["markdown"] = escape_html_attr(item.get("markdown", ""))
        elif item_type == "section":
            card["is_section"] = True
            card["markdown"] = escape_html_attr(item.get("markdown", ""))
        else:
            # Table
            card["is_table"] = True
            card["html"] = item.get("html", "")

        if source_meta and item_type in {"chart", "table"}:
            rows = [
                {"label": "Question", "value": source_meta.get("question", "")},
                {"label": "Tool", "value": source_meta.get("tool", "")},
                {
                    "label": "Rows",
                    "value": (
                        str(source_meta["row_count"])
                        if source_meta.get("row_count") is not None
                        else ""
                    ),
                },
                {
                    "label": "Columns",
                    "value": (
                        str(source_meta["column_count"])
                        if source_meta.get("column_count") is not None
                        else ""
                    ),
                },
                {
                    "label": "Execution",
                    "value": (
                        f"{round(source_meta['execution_time_ms'])} ms"
                        if source_meta.get("execution_time_ms") is not None
                        else ""
                    ),
                },
                {"label": "Chart", "value": source_meta.get("chart_type", "")},
            ]
            card["has_source_meta"] = True
            card["source_rows"] = [row for row in rows if row["value"]]
            if source_meta.get("sql"):
                card["source_sql"] = source_meta["sql"]
            if source_meta.get("error"):
                card["source_error"] = source_meta["error"]

        cards.append(card)

    return cards, chart_specs


_OPERATOR_SYMBOLS = {
    "eq": "=",
    "neq": "≠",
    "gt": ">",
    "gte": "≥",
    "lt": "<",
    "lte": "≤",
    "contains": "contains",
    "in": "in",
}


def _format_filter_value(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(v) for v in value)
    return str(value)


def _format_filter_scope(scope: Any) -> str:
    if not isinstance(scope, dict) or scope.get("type") != "cards":
        return ""
    card_ids = scope.get("cardIds") or []
    if not isinstance(card_ids, list):
        return ""
    n = len(card_ids)
    if n == 0:
        return "no cards"
    return f"{n} card{'' if n == 1 else 's'}"


def _build_filter_chips(filters: list[dict[str, Any]]) -> list[dict[str, str]]:
    chips = []
    for f in filters:
        column = f.get("column")
        if not column:
            continue
        operator = _OPERATOR_SYMBOLS.get(f.get("operator", "eq"), f.get("operator", "="))
        value = _format_filter_value(f.get("value"))
        scope = _format_filter_scope(f.get("scope"))
        chips.append(
            {
                "column": column,
                "operator": operator,
                "value": value,
                "scope": scope,
                "has_scope": bool(scope),
            }
        )
    return chips


def export_dashboard_html(
    items: list[dict[str, Any]],
    title: str = "",
    columns: int = 2,
    filters: list[dict[str, Any]] | None = None,
) -> str:
    """Render dashboard items as a self-contained HTML page with unified Plotly.

    Parameters
    ----------
    items:
        List of dashboard item dicts with keys:
        - type: "chart" or "table"
        - html: For charts, the iframe srcdoc; for tables, the HTML content
        - title: Optional card title
    title:
        Page heading shown above the cards. Empty (default) skips the heading
        — the export carries no title unless the user supplies one.
    columns:
        Number of grid columns (1, 2, or 3).

    Returns
    -------
    A complete HTML string.
    """
    cards, chart_specs = _build_dashboard_cards(items)
    filter_chips = _build_filter_chips(filters or [])

    return render_template(
        "export_dashboard",
        {
            "title": title,  # Let Mustache escape via {{title}}.
            # has_title gates the <h1> via {{#has_title}}...{{/has_title}}. Using
            # the title string itself as the section condition would push it as a
            # new context — {{title}} inside would then resolve to str.title.
            "has_title": bool(title),
            # Browser tab title can't be empty without looking broken — fall back to
            # a neutral label that doesn't editorialize about datasight.
            "html_title": title or "Dashboard",
            "plotly_cdn": PLOTLY_CDN,
            "marked_cdn": MARKED_CDN,
            "dompurify_cdn": DOMPURIFY_CDN,
            "columns": columns,
            "cards": cards,
            "chart_specs_json": json_for_script(chart_specs),
            "has_filters": bool(filter_chips),
            "filter_chips": filter_chips,
        },
    )
