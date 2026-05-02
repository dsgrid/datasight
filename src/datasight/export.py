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
from datasight.templating import escape_html_attr, render_template

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
            # plotly_spec is the unbound template (e.g. x: "column_name") which
            # Plotly cannot render on its own. Fall back to regex-extracting
            # from html for the template-driven dashboard apply path.
            spec = item.get("render_plotly_spec") or item.get("plotly_spec")
            if not isinstance(spec, dict):
                spec = _extract_plotly_spec(item.get("html", ""))
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
    title: str = "datasight dashboard",
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
        Page title.
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
            "title": title,  # Let Mustache escape via {{title}}
            "plotly_cdn": PLOTLY_CDN,
            "marked_cdn": MARKED_CDN,
            "dompurify_cdn": DOMPURIFY_CDN,
            "columns": columns,
            "cards": cards,
            "chart_specs_json": json.dumps(chart_specs),
            "has_filters": bool(filter_chips),
            "filter_chips": filter_chips,
        },
    )
