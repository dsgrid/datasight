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


def filter_tables(
    tables: list[TableInfo], schema_config: dict[str, Any] | None
) -> list[TableInfo]:
    """Apply ``schema.yaml`` filtering to introspected tables.

    Tables absent from ``schema_config['tables']`` are dropped entirely.
    For each listed table, column filtering follows these rules:

    - **``columns`` (exact allowlist).** If provided and non-empty, only
      the listed columns are exposed, in the order given. Unknown names
      are logged and dropped. New columns later added to the DB do *not*
      appear until the user adds them to the list.
    - **``excluded_columns`` (glob denylist).** Supports ``fnmatch``-style
      wildcards (``*``, ``?``, ``[abc]``). Applies when ``columns`` is
      absent or empty: every live column is exposed except those matching
      a pattern. Drift-friendly — new DB columns appear automatically.
    - Both set (non-empty) → ``columns`` wins, ``excluded_columns`` is
      ignored for that table with a warning.
    - Neither set → every column is exposed.
    """
    import fnmatch

    if not schema_config:
        return tables
    entries = schema_config.get("tables")
    if not entries:
        return tables

    by_name = {t.name.lower(): t for t in tables}
    result: list[TableInfo] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or "").strip()
        if not name:
            continue
        table = by_name.get(name.lower())
        if table is None:
            logger.warning(f"schema.yaml lists unknown table: {name}")
            continue

        raw_allow = entry.get("columns") if isinstance(entry.get("columns"), list) else []
        raw_deny = (
            entry.get("excluded_columns")
            if isinstance(entry.get("excluded_columns"), list)
            else []
        )

        if raw_allow and raw_deny:
            logger.warning(
                f"schema.yaml: {table.name} has both 'columns' and "
                f"'excluded_columns'; using 'columns' and ignoring exclusions"
            )

        if raw_allow:
            col_by_name = {c.name.lower(): c for c in table.columns}
            ordered: list[ColumnInfo] = []
            for col in raw_allow:
                key = str(col).strip().lower()
                if not key:
                    continue
                match = col_by_name.get(key)
                if match is None:
                    logger.warning(f"schema.yaml: unknown column {col!r} in table {table.name}")
                    continue
                ordered.append(match)
            if not ordered:
                logger.warning(f"schema.yaml: no valid columns for {table.name}; skipping table")
                continue
            columns_out = ordered
        elif raw_deny:
            patterns = [str(p).strip().lower() for p in raw_deny if str(p).strip()]
            columns_out = [
                c
                for c in table.columns
                if not any(fnmatch.fnmatchcase(c.name.lower(), p) for p in patterns)
            ]
            if not columns_out:
                logger.warning(
                    f"schema.yaml: excluded_columns hides every column of "
                    f"{table.name}; skipping table"
                )
                continue
        else:
            columns_out = list(table.columns)

        hidden = [c.name for c in table.columns if c not in columns_out]
        if hidden:
            preview = ", ".join(hidden[:5])
            more = f" (+{len(hidden) - 5} more)" if len(hidden) > 5 else ""
            logger.info(
                f"schema.yaml: {table.name} exposing {len(columns_out)} of "
                f"{len(table.columns)} columns; {len(hidden)} hidden: {preview}{more}"
            )

        result.append(TableInfo(name=table.name, columns=columns_out, row_count=table.row_count))

    logger.info(f"schema.yaml: filtered to {len(result)} of {len(tables)} tables")
    return result


async def introspect_schema(
    run_sql: RunSql,
    runner: Any = None,
    allowed_tables: set[str] | None = None,
) -> list[TableInfo]:
    """Discover tables and columns from the database.

    Parameters
    ----------
    run_sql:
        Async callable that takes a SQL string and returns a DataFrame.
    runner:
        Optional runner instance. If it has a ``get_table_names`` method,
        that is used for table discovery (e.g. ADBC GetObjects RPC).
    allowed_tables:
        Optional case-insensitive allowlist. When provided, per-table column
        and row-count probes are skipped for tables not in the list — on
        large databases this turns a minutes-long startup into seconds.
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

    total_discovered = len(table_names)
    if allowed_tables is not None:
        allowed_lower = {n.lower() for n in allowed_tables}
        table_names = [t for t in table_names if t.lower() in allowed_lower]
        logger.info(
            f"Introspecting {len(table_names)} of {total_discovered} tables "
            f"(filtered by schema.yaml)"
        )
    else:
        logger.info(f"Introspecting {total_discovered} tables")

    total = len(table_names)
    progress_every = max(1, total // 10) if total >= 20 else total + 1
    for idx, tname in enumerate(table_names, start=1):
        cols = await _get_columns(run_sql, tname, prefer_sqlite=is_sqlite)
        row_count = await _get_row_count(run_sql, tname)
        tables.append(TableInfo(name=tname, columns=cols, row_count=row_count))
        if idx % progress_every == 0 and idx < total:
            logger.info(f"  introspected {idx}/{total} tables")

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
