"""
Shared logic for generating schema_description.md and queries.yaml.

Used by both the CLI `datasight generate` command and the web UI's
Save-as-Project wizard.
"""

from __future__ import annotations

import re

import pandas as pd
from loguru import logger

from datasight.prompts import DESCRIBE_SYSTEM_PROMPT, DESCRIBE_USER_MESSAGE
from datasight.runner import RunSql
from datasight.schema import TableInfo, _validate_identifier, format_schema_context


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
        table_name = _validate_identifier(table.name)
        for col in table.columns:
            base_type = col.dtype.upper().split("(")[0].strip()
            if base_type not in string_types:
                continue
            col_name = _validate_identifier(col.name)
            try:
                count_result = await run_sql(
                    f"SELECT COUNT(DISTINCT {col_name}) AS n FROM {table_name}"
                )
                n_distinct = _extract_scalar(count_result, "n")
                if n_distinct < 1 or n_distinct > 50:
                    continue
                sample_result = await run_sql(
                    f"SELECT DISTINCT {col_name} AS val FROM {table_name} "
                    f"WHERE {col_name} IS NOT NULL ORDER BY {col_name} LIMIT 20"
                )
                values = _extract_values(sample_result, "val")
                if values:
                    lines.append(
                        f"**{table.name}.{col.name}** ({n_distinct} distinct): {', '.join(values)}"
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
    schema_and_samples += f"\n\nSQL dialect: {sql_dialect}\n"

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
