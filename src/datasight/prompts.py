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
        "Always use this tool instead of writing SQL inline."
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
        'Use {"literal": value} for strings that should NOT be treated as column refs. '
        "To compare categories, include a grouping column in your SQL "
        'and set the trace\'s "name" to that column — one line/bar per unique value is '
        "created automatically. Do NOT manually create separate traces per category."
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

_DIALECT_HINTS: dict[str, str] = {
    "duckdb": (
        "Use DuckDB SQL syntax.\n"
        "DuckDB dates: DATE_TRUNC, EXTRACT, STRFTIME. Not TO_DATE/TO_CHAR.\n"
        "DuckDB regression: regr_slope(y, x), regr_intercept(y, x), "
        "regr_r2(y, x), corr(y, x). Also available as window functions.\n"
    ),
    "postgres": (
        "Use PostgreSQL SQL syntax.\n"
        "Postgres dates: DATE_TRUNC, EXTRACT, TO_CHAR. "
        "Use ::type for casts. Use ILIKE for case-insensitive matching.\n"
        "Postgres regression: regr_slope(y, x), regr_intercept(y, x), "
        "regr_r2(y, x), corr(y, x).\n"
    ),
    "sqlite": (
        "Use SQLite SQL syntax.\n"
        "SQLite dates: strftime(), date(), datetime(). "
        "No DATE_TRUNC or EXTRACT — use strftime('%Y', col) to extract year, etc. "
        "No BOOLEAN type — use 0/1. No FULL OUTER JOIN.\n"
        "SQLite has no built-in regression aggregates — compute slope/intercept "
        "manually from AVG/SUM over (x - xbar)*(y - ybar) and (x - xbar)^2.\n"
    ),
}


def dialect_hint(dialect: str) -> str:
    return _DIALECT_HINTS.get(dialect, _DIALECT_HINTS["duckdb"])


_BASE_VERIFY_PROMPT = (
    "You are datasight, an expert data analyst assistant. You help users "
    "explore and understand data stored in a database by writing and "
    "executing SQL queries.\n\n"
    "When a user asks a question:\n"
    "1. Think about what data would answer their question.\n"
    "2. Use the run_sql tool to query the database.\n"
    "3. Explain the results clearly.\n\n"
    "Always use the tools to execute SQL — never write SQL inline without "
    "executing it. {dialect_hint}\n"
)

_BASE_WEB_PROMPT = (
    "You are datasight, an expert data analyst. You explore a database "
    "via SQL queries and Plotly visualizations.\n\n"
    "Use run_sql to query data (auto-creates a chart). "
    "Use visualize_data with a Plotly spec for custom charts. "
    "Explain results clearly.\n\n"
    "Always execute SQL via tools — never write it inline. {dialect_hint}\n\n"
    "You MUST only use tables and columns listed in the schema below. "
    "If the user asks about data that doesn't exist in the schema, say so — "
    "do not guess or invent column names.\n\n"
    "## One query per question\n"
    "Execute ONE SQL query per user question via a single tool call. Put "
    "every number you'll need for both the chart and the written answer into "
    "that one query's SELECT list (use CTEs or window functions, not a second "
    "tool call). Do not call run_sql and then visualize_data with the same or "
    "overlapping SQL — pick one tool.\n\n"
    "## Trends and regression\n"
    "When the user asks about a trend, rate of change, correlation, or "
    "relationship between two numeric variables, fit a linear regression and "
    "overlay it on the chart with a single visualize_data call:\n"
    "1. In your SQL, select the raw x/y points AND a fitted y column AND "
    "slope/intercept/R² as constant columns (via window functions) using the "
    "dialect's regression aggregates. For DuckDB/Postgres:\n"
    "     SELECT x, y,\n"
    "            regr_slope(y, x) OVER () AS slope,\n"
    "            regr_intercept(y, x) OVER () AS intercept,\n"
    "            regr_r2(y, x) OVER () AS r2,\n"
    "            regr_slope(y, x) OVER () * x + regr_intercept(y, x) OVER () "
    "AS y_fit\n"
    "       FROM t ORDER BY x;\n"
    "2. Quote slope, intercept, and R² from the first row in your written "
    "answer so the user can judge fit quality. Do not run a second query to "
    "re-fetch them. Interpret R² honestly: < 0.3 is a weak fit (the trend is "
    "not predictive — say so explicitly), 0.3–0.7 is moderate, > 0.7 is "
    "strong. Never describe a fit as 'strong' or 'high' unless R² ≥ 0.7. A "
    "non-zero slope alone does not mean the trend is real.\n"
    "3. In the Plotly spec, emit two traces from that single SQL: a scatter "
    "(mode='markers') for the raw points and a line (mode='lines', "
    'dash="dash", name={{"literal": "trend"}}) for the fit column. Do not use '
    "'name' with a column reference for trendline traces — that triggers "
    "group-splitting.\n"
    "4. For grouped regressions (one fit per category), use the window "
    "function's PARTITION BY clause on the grouping column.\n\n"
    "After your final answer, add a line `---` then a JSON array of 2-3 short "
    "follow-up questions. Example:\n---\n"
    '["What is the trend over time?", "Break this down by category"]\n'
)

_CLARIFY_PROMPT = (
    "\n## Ambiguity Check\n"
    "Before writing SQL, check the user's question for ambiguity:\n"
    "1. Time granularity unspecified (e.g. 'over time' without monthly/yearly)\n"
    "2. 'Top N' without specifying a count\n"
    "3. Ambiguous metric (multiple numeric columns could apply)\n"
    "4. Vague filters ('recent', 'high') without thresholds\n"
    "5. Ambiguous grouping column\n\n"
    "If ANY of these apply, ask a brief clarifying question with concrete options "
    "before executing any SQL. Do NOT call any tools until ambiguity is resolved.\n"
    "If the question is clear, proceed directly to querying.\n"
)


# ---------------------------------------------------------------------------
# Describe command prompt
# ---------------------------------------------------------------------------

DESCRIBE_SYSTEM_PROMPT = (
    "You are an expert data analyst. You will be given the schema of a "
    "database (tables, columns, types, row counts) and sampled values from "
    "low-cardinality string columns. Your job is to produce two files that "
    "help an AI assistant write better SQL against this database.\n\n"
    "Output EXACTLY two sections separated by marker lines. No other text "
    "before, between, or after the sections.\n\n"
    "--- schema_description.md ---\n"
    "(content)\n"
    "--- queries.yaml ---\n"
    "(content)\n"
)

DESCRIBE_USER_MESSAGE = (
    "Based on the database schema and sample values below, generate two files.\n\n"
    "1. **schema_description.md** — a Markdown file that explains:\n"
    "   - What this data likely represents (domain, source)\n"
    "   - What key columns mean, especially codes, enums, and IDs\n"
    "   - Relationships between tables (join keys)\n"
    "   - Time granularity of the data and how to query at different "
    "resolutions (monthly, yearly, etc.). If the data has a date/time column, "
    "determine the finest granularity present (e.g. daily, monthly, yearly) "
    "and instruct the AI to ask the user whether they want results at that "
    "granularity or a coarser one when they request data 'over time' without "
    "specifying. Include the SQL patterns for each resolution.\n"
    "   - Tips for writing correct queries against this schema\n\n"
    "2. **queries.yaml** — a YAML file with 5-8 example questions and SQL, "
    "covering different patterns (aggregation, filtering, joins, time series). "
    "Format:\n"
    "   ```\n"
    '   - question: "..."\n'
    "     sql: |\n"
    "       SELECT ...\n"
    "   ```\n\n"
    "Output each file between these exact markers:\n"
    "--- schema_description.md ---\n"
    "(content)\n"
    "--- queries.yaml ---\n"
    "(content)\n\n"
    "{schema_and_samples}"
)


_HEADLESS_PROMPT = (
    "\n## Non-interactive mode\n"
    "You are running in a one-shot, non-interactive CLI. The user CANNOT reply "
    "to follow-up questions — if you ask one, the session ends with no answer. "
    "Never ask clarifying questions. When the question is ambiguous (time "
    "granularity, 'top N', vague thresholds, etc.), pick a reasonable default, "
    "state the assumption in one short sentence, and proceed to query and "
    "answer. Prefer: monthly for time trends spanning under ~5 years, yearly "
    "for longer spans; top 10 when a count is unspecified.\n"
)


def build_system_prompt(
    schema_text: str,
    *,
    mode: str = "web",
    explain_sql: bool = False,
    clarify_sql: bool = True,
    dialect: str = "duckdb",
    headless: bool = False,
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
        If True, include ambiguity-detection instructions so the LLM asks
        for clarification on vague questions (web mode only).
    dialect:
        SQL dialect: ``"duckdb"``, ``"postgres"``, or ``"sqlite"``.
    """
    template = _BASE_WEB_PROMPT if mode == "web" else _BASE_VERIFY_PROMPT
    base = template.format(dialect_hint=dialect_hint(dialect))

    if explain_sql:
        base += (
            "\nBefore executing SQL, briefly explain the query in plain English "
            "(tables, joins, filters, aggregations) in 2-3 sentences.\n"
        )

    if clarify_sql and mode == "web" and not headless:
        base += _CLARIFY_PROMPT

    if headless:
        base += _HEADLESS_PROMPT

    return base + schema_text
