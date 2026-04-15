"""
Auto-discover database schema by querying INFORMATION_SCHEMA or DuckDB-specific
system tables.
"""

from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from loguru import logger

from datasight.runner import RunSql, SQLiteRunner


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


async def introspect_schema(run_sql: RunSql, runner: Any = None) -> list[TableInfo]:
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
    is_sqlite = _is_sqlite_runner(runner)
    if not table_names:
        table_names = await _get_table_names(run_sql, prefer_sqlite=is_sqlite)
    if not table_names:
        logger.warning("No tables found in database")
        return tables

    for tname in table_names:
        cols = await _get_columns(run_sql, tname, prefer_sqlite=is_sqlite)
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
    parts.append("- Only SELECT columns that exist in your FROM tables. JOIN if needed.")
    parts.append("- For 'top N', use a CTE/subquery to find the top N first.")
    parts.append("- For charts: SELECT 2-3 columns (category/date + numeric).")
    parts.append("")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _run(run_sql: RunSql, sql: str) -> pd.DataFrame:
    """Run a query, returning an empty DataFrame on error."""
    try:
        return await run_sql(sql)
    except Exception as e:
        logger.trace(f"Schema probe failed: {sql!r} - {e}")
        return pd.DataFrame()


def _is_sqlite_runner(runner: Any) -> bool:
    """Return True when ``runner`` is or wraps a SQLiteRunner."""
    current = runner
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        if isinstance(current, SQLiteRunner):
            return True
        seen.add(id(current))
        current = getattr(current, "_inner", None)
    return False


async def _get_table_names(run_sql: RunSql, *, prefer_sqlite: bool = False) -> list[str]:
    """Get table names, trying multiple strategies."""
    if prefer_sqlite:
        names = await _get_sqlite_table_names(run_sql)
        if names:
            return names

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

    names = await _get_sqlite_table_names(run_sql)
    if names:
        return names

    return []


async def _get_sqlite_table_names(run_sql: RunSql) -> list[str]:
    """Get SQLite table and view names."""
    df = await _run(
        run_sql,
        "SELECT name FROM sqlite_master "
        "WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name",
    )
    if not df.empty:
        return df.iloc[:, 0].tolist()
    return []


def _validate_identifier(name: str) -> str:
    """Validate that a name is a safe SQL identifier (alphanumeric + underscores).

    Raises ValueError for names containing characters that could enable injection.
    """
    if not all(c.isalnum() or c in ("_", "-", ".") for c in name):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return name


async def _get_columns(
    run_sql: RunSql, table: str, *, prefer_sqlite: bool = False
) -> list[ColumnInfo]:
    """Get column info for a table."""
    _validate_identifier(table)

    if prefer_sqlite:
        cols = await _get_sqlite_columns(run_sql, table)
        if cols:
            return cols

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

    # Fallback: INFORMATION_SCHEMA (identifier already validated above)
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

    cols = await _get_sqlite_columns(run_sql, table)
    if cols:
        return cols

    df = await _run(run_sql, f'SELECT * FROM "{table}" LIMIT 0')
    if not df.empty or len(df.columns) > 0:
        return [ColumnInfo(name=c, dtype="UNKNOWN") for c in df.columns]

    return []


async def _get_sqlite_columns(run_sql: RunSql, table: str) -> list[ColumnInfo]:
    """Get SQLite column info for a table or view."""
    df = await _run(run_sql, f'PRAGMA table_info("{table}")')
    if not df.empty and "name" in df.columns:
        cols = []
        for _, row in df.iterrows():
            cols.append(
                ColumnInfo(
                    name=row.get("name", ""),
                    dtype=row.get("type", "TEXT"),
                    nullable=row.get("notnull", 0) == 0,
                )
            )
        return cols

    return []


async def _get_row_count(run_sql: RunSql, table: str) -> int | None:
    """Get approximate row count."""
    df = await _run(run_sql, f'SELECT COUNT(*) AS cnt FROM "{table}"')
    if not df.empty:
        try:
            return int(df.iloc[0, 0])
        except (ValueError, TypeError):
            pass
    return None
