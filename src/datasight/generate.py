"""
Shared logic for generating schema_description.md and queries.yaml.

Used by both the CLI `datasight generate` command and the web UI's
Save-as-Project wizard.
"""

from __future__ import annotations

import re

import pandas as pd
from loguru import logger

from datasight.prompts import DESCRIBE_SYSTEM_PROMPT, DESCRIBE_USER_MESSAGE, dialect_hint
from datasight.runner import RunSql
from datasight.schema import TableInfo, _quote_identifier, format_schema_context

_TEMPORAL_TYPES = {
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "TIMESTAMP WITH TIME ZONE",
    "TIMESTAMP WITHOUT TIME ZONE",
    "DATE",
    "DATETIME",
    "TIME",
}
_INTEGER_TYPES = {
    "BIGINT",
    "INTEGER",
    "INT",
    "INT2",
    "INT4",
    "INT8",
    "SMALLINT",
    "HUGEINT",
    "UBIGINT",
    "UINTEGER",
    "USMALLINT",
    "UHUGEINT",
}
_TIME_NAME_HINTS = ("time", "stamp", "date", "epoch")


async def sample_enum_columns(run_sql: RunSql, tables: list[TableInfo]) -> str:
    """Sample distinct values from low-cardinality string columns.

    Identifies string-type columns with <= 50 distinct values and samples
    up to 20 values from each. This helps the LLM understand code/enum columns.

    Parameters
    ----------
    run_sql:
        Async callable that executes SQL and returns a DataFrame.
    tables:
        List of TableInfo objects from schema introspection.

    Returns
    -------
    Formatted markdown string with sampled values per column.
    """
    lines: list[str] = []
    string_types = {"VARCHAR", "TEXT", "CHAR", "STRING", "NVARCHAR", "BPCHAR", "NAME"}

    for table in tables:
        try:
            table_name = _quote_identifier(table.name)
        except ValueError:
            continue
        for col in table.columns:
            base_type = col.dtype.upper().split("(")[0].strip()
            if base_type not in string_types:
                continue
            try:
                col_name = _quote_identifier(col.name)
            except ValueError:
                continue
            try:
                count_result = await run_sql(
                    f"SELECT COUNT(DISTINCT {col_name}) AS n FROM {table_name}"
                )
                n_distinct = _extract_scalar(count_result, "n")
                if n_distinct < 1 or n_distinct > 50:
                    continue
                # ORDER BY the output alias (not the pre-DISTINCT column) so
                # this parses under Spark's ANSI analyzer, which requires an
                # ORDER BY after DISTINCT to reference output-list columns.
                # DuckDB and Postgres accept either form.
                sample_result = await run_sql(
                    f"SELECT DISTINCT {col_name} AS val FROM {table_name} "
                    f"WHERE {col_name} IS NOT NULL ORDER BY val LIMIT 20"
                )
                values = _extract_values(sample_result, "val")
                if values:
                    lines.append(
                        f"**{table.name}.{col.name}** ({n_distinct} distinct): {', '.join(values)}"
                    )
            except Exception:
                continue

    return "\n".join(lines)


async def sample_timestamp_columns(run_sql: RunSql, tables: list[TableInfo]) -> str:
    """Sample min/max from timestamp-like columns.

    Shows the LLM the actual range of temporal columns and, for integer
    columns with time-suggestive names, the magnitude — so it can infer
    whether values are seconds, milliseconds, or microseconds since epoch.
    """
    lines: list[str] = []

    for table in tables:
        try:
            table_name = _quote_identifier(table.name)
        except ValueError:
            continue
        for col in table.columns:
            base_type = col.dtype.upper().split("(")[0].strip()
            name_lower = col.name.lower()
            is_temporal = base_type in _TEMPORAL_TYPES
            is_numeric_time = base_type in _INTEGER_TYPES and any(
                h in name_lower for h in _TIME_NAME_HINTS
            )
            if not (is_temporal or is_numeric_time):
                continue
            try:
                col_name = _quote_identifier(col.name)
            except ValueError:
                continue
            try:
                result = await run_sql(
                    f"SELECT MIN({col_name}) AS min_v, MAX({col_name}) AS max_v "
                    f"FROM {table_name} WHERE {col_name} IS NOT NULL"
                )
                if result.empty:
                    continue
                min_v = result.iloc[0]["min_v"]
                max_v = result.iloc[0]["max_v"]
                if pd.isna(min_v) or pd.isna(max_v):
                    continue
                suffix = ""
                if is_numeric_time:
                    suffix = (
                        " — integer column; infer unit from magnitude "
                        "(~1e9 = seconds, ~1e12 = milliseconds, "
                        "~1e15 = microseconds since Unix epoch)"
                    )
                lines.append(
                    f"**{table.name}.{col.name}** ({col.dtype}): min={min_v}, max={max_v}{suffix}"
                )
            except Exception:
                continue

    return "\n".join(lines)


def _extract_scalar(df: pd.DataFrame, col: str) -> int:
    """Extract a scalar integer from a DataFrame result."""
    if df.empty:
        return 0
    try:
        return int(df.iloc[0][col])
    except (KeyError, ValueError, TypeError):
        return 0


def _extract_values(df: pd.DataFrame, col: str) -> list[str]:
    """Extract a list of string values from a DataFrame column."""
    if df.empty:
        return []
    try:
        return [str(v) for v in df[col].dropna().tolist() if v]
    except KeyError:
        return []


def build_generation_context(
    tables: list[TableInfo],
    sql_dialect: str,
    samples_text: str,
    user_description: str | None = None,
    timestamps_text: str = "",
) -> tuple[str, str]:
    """Build the system prompt and user message for LLM documentation generation.

    Parameters
    ----------
    tables:
        Introspected table info.
    sql_dialect:
        SQL dialect name (e.g., "duckdb").
    samples_text:
        Output from sample_enum_columns().
    user_description:
        Optional user-provided description of the data.
    timestamps_text:
        Output from sample_timestamp_columns(). Helps the LLM infer epoch
        unit and pick dialect-correct time functions.

    Returns
    -------
    Tuple of (system_prompt, user_message).
    """
    schema_text = format_schema_context(tables)

    schema_and_samples = ""
    if user_description:
        schema_and_samples += f"## User-Provided Context\n\n{user_description}\n\n"
    schema_and_samples += schema_text
    if samples_text:
        schema_and_samples += "\n\n## Sampled Column Values\n\n" + samples_text
    if timestamps_text:
        schema_and_samples += "\n\n## Timestamp Column Ranges\n\n" + timestamps_text
    schema_and_samples += (
        f"\n\n## SQL Dialect: {sql_dialect}\n\n{dialect_hint(sql_dialect)}"
        "All SQL you emit (including time-resolution patterns in "
        "schema_description.md and every query in queries.yaml) MUST use "
        f"functions valid in {sql_dialect}. Do not use functions from other "
        "dialects (e.g. FROM_UNIXTIME, DATE_FORMAT, YEAR() as a function) "
        "unless they are valid in this dialect.\n"
    )

    user_msg = DESCRIBE_USER_MESSAGE.format(schema_and_samples=schema_and_samples)
    return DESCRIBE_SYSTEM_PROMPT, user_msg


def parse_generation_response(text: str) -> tuple[str | None, str | None]:
    """Parse LLM response into schema_description and queries content.

    Splits on ``--- schema_description.md ---`` and ``--- queries.yaml ---``
    markers.

    Parameters
    ----------
    text:
        Raw LLM response text.

    Returns
    -------
    Tuple of (schema_content, queries_content). Either may be None if
    parsing fails.
    """
    if "--- schema_description.md ---" in text and "--- queries.yaml ---" in text:
        parts = text.split("--- schema_description.md ---", 1)
        rest = parts[1]
        schema_queries = rest.split("--- queries.yaml ---", 1)
        schema_content = schema_queries[0].strip()
        queries_content = schema_queries[1].strip()

        # Strip YAML document separators and markdown fences that LLMs sometimes add
        if queries_content:
            queries_content = _clean_yaml_content(queries_content)

        return schema_content or None, queries_content or None

    logger.warning("Could not parse LLM response into separate files")
    stripped = text.strip()
    return stripped or None, None


def _clean_yaml_content(content: str) -> str:
    """Clean up LLM-generated YAML content.

    Removes YAML document separators (---) that cause multi-document parse errors,
    and strips markdown code fences that LLMs sometimes wrap output in.
    """
    # Strip markdown code fences
    content = re.sub(r"^```(?:yaml|yml)?\s*\n", "", content)
    content = re.sub(r"\n```\s*$", "", content)

    # Remove YAML document separators (--- on its own line)
    lines = content.split("\n")
    cleaned = [line for line in lines if line.strip() != "---"]
    return "\n".join(cleaned)
