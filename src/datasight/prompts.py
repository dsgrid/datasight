"""
Shared system prompt construction and tool definitions for datasight.

Centralizes the base prompt, tool schemas, and prompt-building logic
used by both the web app and the verify CLI command.
"""

from typing import Any


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

RUN_SQL_TOOL: dict[str, Any] = {
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
}

VISUALIZE_DATA_TOOL: dict[str, Any] = {
    "name": "visualize_data",
    "description": (
        "Execute SQL and render as a Plotly.js chart. Any Plotly chart type is supported. "
        "String values in traces matching SQL column names are replaced with data arrays. "
        'Use {"literal": value} for strings that should NOT be treated as column refs.'
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "SQL SELECT query to fetch data",
            },
            "title": {
                "type": "string",
                "description": "Chart title",
            },
            "plotly_spec": {
                "type": "object",
                "description": (
                    "Plotly.js spec with 'data' and 'layout' keys. "
                    'Example: {"data": [{"type": "bar", "x": "category", "y": "total"}], '
                    '"layout": {}}'
                ),
                "properties": {
                    "data": {
                        "type": "array",
                        "description": "Array of Plotly trace objects",
                        "items": {"type": "object"},
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
}

# Web app exposes both tools; verify only exposes run_sql.
WEB_TOOLS: list[dict[str, Any]] = [RUN_SQL_TOOL, VISUALIZE_DATA_TOOL]
VERIFY_TOOLS: list[dict[str, Any]] = [RUN_SQL_TOOL]


# ---------------------------------------------------------------------------
# System prompt construction
# ---------------------------------------------------------------------------

_BASE_VERIFY_PROMPT = (
    "You are datasight, an expert data analyst assistant. You help users "
    "explore and understand data stored in a DuckDB database by writing and "
    "executing SQL queries.\n\n"
    "When a user asks a question:\n"
    "1. Think about what data would answer their question.\n"
    "2. Use the run_sql tool to query the database.\n"
    "3. Explain the results clearly.\n\n"
    "Always use the tools to execute SQL — never write SQL inline without "
    "executing it. Use DuckDB SQL syntax.\n"
)

_BASE_WEB_PROMPT = (
    "You are datasight, an expert data analyst. You explore a DuckDB database "
    "via SQL queries and Plotly visualizations.\n\n"
    "1. Use run_sql to query data (auto-creates a chart).\n"
    "2. Use visualize_data with a Plotly spec for custom charts.\n"
    "3. Explain results clearly.\n\n"
    "Always execute SQL via tools — never write it inline. Use DuckDB syntax.\n\n"
    "After your final answer, add a line `---` then a JSON array of 2-3 short "
    "follow-up questions. Example:\n---\n"
    '["What is the trend over time?", "Break this down by category"]\n'
)


def build_system_prompt(
    schema_text: str,
    *,
    mode: str = "web",
    explain_sql: bool = False,
    clarify_sql: bool = True,
) -> str:
    """Build a complete system prompt.

    Parameters
    ----------
    schema_text:
        Pre-formatted schema context (from ``format_schema_context`` +
        ``format_example_queries``).
    mode:
        ``"web"`` for the interactive web UI, ``"verify"`` for the
        verification CLI.
    explain_sql:
        If True, instruct the LLM to explain queries before executing.
    clarify_sql:
        If True, instruct the LLM to ask clarifying questions for
        ambiguous queries (web mode only).
    """
    base = _BASE_WEB_PROMPT if mode == "web" else _BASE_VERIFY_PROMPT

    if explain_sql:
        base += (
            "\nBefore executing SQL, briefly explain the query in plain English "
            "(tables, joins, filters, aggregations) in 2-3 sentences.\n"
        )
    if clarify_sql and mode == "web":
        base += (
            "\nCLARIFICATION: Before writing SQL, ask the user to clarify if any apply:\n"
            "1. Time granularity unspecified (daily/monthly/yearly?)\n"
            "2. 'Top N' without a count\n"
            "3. Ambiguous metric (multiple numeric columns possible)\n"
            "4. Vague filters ('recent', 'high') without thresholds\n"
            "5. Ambiguous grouping column\n"
            "Format: short question + markdown list with **bold** options.\n"
            "If none apply, proceed directly.\n"
        )

    return base + schema_text
