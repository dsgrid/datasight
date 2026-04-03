"""
Auto-discover database schema by querying INFORMATION_SCHEMA or DuckDB-specific
system tables.
"""

from dataclasses import dataclass, field

import pandas as pd
from loguru import logger


@dataclass
class ColumnInfo:
    name: str
    dtype: str
    nullable: bool = True


@dataclass
class TableInfo:
    name: str
    columns: list[ColumnInfo] = field(default_factory=list)
    row_count: int | None = None


async def introspect_schema(run_sql, runner=None) -> list[TableInfo]:
    """Discover tables and columns from the database.

    Parameters
    ----------
    run_sql:
        Async callable that takes a SQL string and returns a DataFrame.
    runner:
        Optional runner instance. If it has a ``get_table_names`` method,
        that is used for table discovery (e.g. ADBC GetObjects RPC).
    """
    tables: list[TableInfo] = []

    table_names: list[str] = []
    if runner and hasattr(runner, "get_table_names"):
        try:
            table_names = runner.get_table_names()
            logger.info(f"ADBC GetObjects discovered {len(table_names)} tables")
        except Exception as e:
            logger.info(f"ADBC GetObjects failed, falling back to SQL: {e}")
    if not table_names:
        table_names = await _get_table_names(run_sql)
    if not table_names:
        logger.warning("No tables found in database")
        return tables

    for tname in table_names:
        cols = await _get_columns(run_sql, tname)
        row_count = await _get_row_count(run_sql, tname)
        tables.append(TableInfo(name=tname, columns=cols, row_count=row_count))

    return tables


def format_schema_context(
    tables: list[TableInfo],
    user_description: str | None = None,
) -> str:
    """Format discovered schema + user description into a system prompt section."""
    parts = ["\n## Database Schema\n"]

    if user_description:
        parts.append(user_description)
        parts.append("")

    parts.append("### Tables\n")
    for t in tables:
        row_str = f" — {t.row_count:,} rows" if t.row_count is not None else ""
        parts.append(f"**{t.name}**{row_str}")
        if t.columns:
            parts.append("Columns:")
            for c in t.columns:
                null = "" if c.nullable else " NOT NULL"
                parts.append(f"  {c.name} ({c.dtype}{null})")
        parts.append("")

    parts.append("### Guidelines")
    parts.append("- IMPORTANT: Always use the run_sql tool to execute queries. Never write SQL")
    parts.append("  inline in your response — always call the tool so the user sees real results.")
    parts.append("- A chart is automatically created when run_sql returns results.")
    parts.append("- When writing SQL for visualization, SELECT only 2-3 columns (e.g. one")
    parts.append("  category/date column and one numeric column) for best chart results.")
    parts.append("- IMPORTANT: Only SELECT columns that exist in the tables you are querying.")
    parts.append("  Check the schema above carefully. If a column is in a different table,")
    parts.append("  you MUST JOIN that table first.")
    parts.append("- When the user asks for 'top N', use a CTE or subquery to find the top N")
    parts.append("  first, then filter the main query to only include those results.")
    parts.append("- Use DuckDB SQL syntax. For dates use: DATE_TRUNC('month', col),")
    parts.append(
        "  EXTRACT(YEAR FROM col), STRFTIME(col, '%Y-%m'). Do NOT use TO_DATE or TO_CHAR."
    )
    parts.append("- For visualizations: bar charts for comparisons, line charts for trends,")
    parts.append("  pie/donut for composition. Always label axes with units.")
    parts.append("")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _run(run_sql, sql: str) -> pd.DataFrame:
    """Run a query, returning an empty DataFrame on error."""
    try:
        return await run_sql(sql)
    except Exception as e:
        logger.info(f"Query failed: {sql!r} — {e}")
        return pd.DataFrame()


async def _get_table_names(run_sql) -> list[str]:
    """Get table names, trying multiple strategies."""
    df = await _run(run_sql, "SHOW TABLES")
    if not df.empty and len(df.columns) > 0:
        return df.iloc[:, 0].tolist()

    df = await _run(
        run_sql,
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('information_schema', 'pg_catalog') "
        "ORDER BY table_name",
    )
    if not df.empty:
        return df.iloc[:, 0].tolist()

    df = await _run(run_sql, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    if not df.empty:
        return df.iloc[:, 0].tolist()

    return []


async def _get_columns(run_sql, table: str) -> list[ColumnInfo]:
    """Get column info for a table."""
    df = await _run(run_sql, f'DESCRIBE "{table}"')
    if not df.empty and "column_name" in df.columns:
        cols = []
        for _, row in df.iterrows():
            cols.append(
                ColumnInfo(
                    name=row.get("column_name", ""),
                    dtype=row.get("column_type", ""),
                    nullable=row.get("null", "YES") == "YES",
                )
            )
        return cols

    quoted = table.replace("'", "''")
    df = await _run(
        run_sql,
        f"SELECT column_name, data_type, is_nullable "
        f"FROM information_schema.columns "
        f"WHERE table_name = '{quoted}' "
        f"ORDER BY ordinal_position",
    )
    if not df.empty:
        cols = []
        for _, row in df.iterrows():
            cols.append(
                ColumnInfo(
                    name=row.get("column_name", ""),
                    dtype=row.get("data_type", ""),
                    nullable=row.get("is_nullable", "YES") == "YES",
                )
            )
        return cols

    df = await _run(run_sql, f'SELECT * FROM "{table}" LIMIT 0')
    if not df.empty or len(df.columns) > 0:
        return [ColumnInfo(name=c, dtype="UNKNOWN") for c in df.columns]

    return []


async def _get_row_count(run_sql, table: str) -> int | None:
    """Get approximate row count."""
    df = await _run(run_sql, f'SELECT COUNT(*) AS cnt FROM "{table}"')
    if not df.empty:
        try:
            return int(df.iloc[0, 0])
        except (ValueError, TypeError):
            pass
    return None
