"""
Export a datasight conversation session as a self-contained HTML page.

Renders user messages, assistant responses, SQL results (tables), and
Plotly charts into a single shareable HTML file.
"""

from __future__ import annotations

from typing import Any

PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.2.min.js"
MARKED_CDN = "https://cdn.jsdelivr.net/npm/marked/marked.min.js"
HLJS_CDN = "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"
HLJS_SQL_CDN = "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/sql.min.js"
HLJS_CSS = "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github.min.css"
DOMPURIFY_CDN = "https://cdn.jsdelivr.net/npm/dompurify@3/dist/purify.min.js"


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
        Optional set of message indices (0-based, counting only
        user_message and assistant_message events) to exclude from
        the export.

    Returns
    -------
    A complete HTML string.
    """
    if exclude_indices is None:
        exclude_indices = set()

    # Group events into message blocks for exclusion tracking
    blocks: list[dict[str, Any]] = []
    msg_idx = 0
    current_block: dict[str, Any] | None = None

    for evt in events:
        etype = evt.get("event")

        if etype == "user_message":
            if current_block:
                blocks.append(current_block)
            current_block = {
                "msg_idx": msg_idx,
                "events": [evt],
            }
            msg_idx += 1
        elif etype == "assistant_message":
            if current_block is None:
                current_block = {"msg_idx": msg_idx, "events": []}
            current_block["events"].append(evt)
            blocks.append(current_block)
            current_block = {"msg_idx": msg_idx + 1, "events": []}
            msg_idx += 1
        elif etype in ("tool_start", "tool_result", "tool_done"):
            if current_block is None:
                current_block = {"msg_idx": msg_idx, "events": []}
            current_block["events"].append(evt)
        # Skip suggestions and other events

    if current_block and current_block["events"]:
        blocks.append(current_block)

    # Build HTML content
    content_parts: list[str] = []
    for block in blocks:
        if block["msg_idx"] in exclude_indices:
            continue
        for evt in block["events"]:
            etype = evt.get("event")
            data = evt.get("data", {})

            if etype == "user_message":
                text = _escape(data.get("text", ""))
                content_parts.append(
                    f'<div class="msg user"><div class="msg-label">You</div>'
                    f'<div class="msg-text">{text}</div></div>'
                )
            elif etype == "assistant_message":
                text = data.get("text", "")
                content_parts.append(
                    f'<div class="msg assistant"><div class="msg-label">datasight</div>'
                    f'<div class="msg-text markdown" data-md="{_escape_attr(text)}"></div></div>'
                )
            elif etype == "tool_result":
                html = data.get("html", "")
                rtype = data.get("type", "table")
                if rtype == "chart" and html:
                    # Embed chart as iframe with srcdoc
                    content_parts.append(
                        f'<div class="result chart-result">'
                        f'<iframe sandbox="allow-scripts allow-same-origin" '
                        f'srcdoc="{_escape_attr(html)}"></iframe></div>'
                    )
                elif html:
                    content_parts.append(f'<div class="result table-result">{html}</div>')
            elif etype == "tool_start":
                sql = data.get("input", {}).get("sql", "")
                if sql:
                    content_parts.append(
                        f'<div class="sql-block"><pre><code class="language-sql">'
                        f"{_escape(sql)}</code></pre></div>"
                    )

    body = "\n".join(content_parts)

    return f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{_escape(title)}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<link rel="stylesheet" href="{HLJS_CSS}">
<script src="{HLJS_CDN}"></script>
<script src="{HLJS_SQL_CDN}"></script>
<script src="{MARKED_CDN}"></script>
<script src="{DOMPURIFY_CDN}"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: 'Space Grotesk', system-ui, sans-serif;
    background: #f8f9fa; color: #1a1a2e;
    max-width: 900px; margin: 0 auto; padding: 32px 24px;
    line-height: 1.6;
  }}
  h1 {{
    font-size: 1.5rem; font-weight: 600; margin-bottom: 24px;
    padding-bottom: 12px; border-bottom: 2px solid #15a8a8;
    color: #15a8a8;
  }}
  .msg {{ margin-bottom: 20px; }}
  .msg-label {{
    font-size: 0.75rem; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.05em; margin-bottom: 4px; color: #666;
  }}
  .msg.user .msg-label {{ color: #15a8a8; }}
  .msg.user .msg-text {{
    background: #e8f5f5; border-radius: 12px; padding: 12px 16px;
    white-space: pre-wrap;
  }}
  .msg.assistant .msg-text {{
    background: white; border: 1px solid #e5e7eb; border-radius: 12px;
    padding: 12px 16px;
  }}
  .msg.assistant .msg-text p {{ margin: 0.5em 0; }}
  .msg.assistant .msg-text p:first-child {{ margin-top: 0; }}
  .msg.assistant .msg-text p:last-child {{ margin-bottom: 0; }}
  .sql-block {{
    margin: 12px 0; background: #f1f3f5; border-radius: 8px;
    overflow: hidden;
  }}
  .sql-block pre {{ margin: 0; padding: 12px 16px; overflow-x: auto; }}
  .sql-block code {{ font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; }}
  .result {{ margin: 12px 0; }}
  .chart-result iframe {{
    width: 100%; height: 480px; border: 1px solid #e5e7eb;
    border-radius: 8px; background: white;
  }}
  .table-result {{ overflow-x: auto; }}
  .table-result table {{
    width: 100%; border-collapse: collapse; font-size: 0.85rem;
  }}
  .table-result th {{
    background: #f1f3f5; padding: 8px 12px; text-align: left;
    font-weight: 600; border-bottom: 2px solid #dee2e6;
  }}
  .table-result td {{
    padding: 6px 12px; border-bottom: 1px solid #e5e7eb;
  }}
  .table-result tr:hover td {{ background: #f8f9fa; }}
  .table-toolbar, .table-pagination {{ display: none; }}
  .footer {{
    margin-top: 40px; padding-top: 16px; border-top: 1px solid #dee2e6;
    font-size: 0.8rem; color: #999; text-align: center;
  }}
</style>
</head>
<body>
<h1>{_escape(title)}</h1>
{body}
<div class="footer">Exported from datasight</div>
<script>
  document.querySelectorAll('.markdown').forEach(function(el) {{
    var md = el.getAttribute('data-md');
    if (md) {{
      el.innerHTML = DOMPurify.sanitize(marked.parse(md));
      el.querySelectorAll('pre code').forEach(function(block) {{
        hljs.highlightElement(block);
      }});
    }}
  }});
  document.querySelectorAll('pre code.language-sql').forEach(function(block) {{
    hljs.highlightElement(block);
  }});
</script>
</body>
</html>"""


def _escape(text: str) -> str:
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


def _escape_attr(text: str) -> str:
    return _escape(text).replace("'", "&#x27;")
