"""Emit previewable cleanup SQL for findings from ``build_quality_overview``.

Mirrors the ``tidy`` module's preview-first ethos: each function returns a
single SQL string that lets the user *see* the rows in question, never an
UPDATE/DELETE that auto-mutates the table. The returned SQL is safe to copy
into a query window or attach to a quality report.

For destructive operations (deduplication, NULLIF rewrites, TRIM rewrites)
the preview is a ``SELECT`` that shows the candidate rows or the rewritten
column alongside the original; the caller can wrap it in an
``UPDATE``/``CREATE OR REPLACE TABLE`` once they've reviewed the preview.

Dialects supported: ``duckdb``, ``sqlite``, ``postgres``. Where a dialect
lacks a feature (e.g. SQLite has no ``QUALIFY`` and no percentile
aggregates), the function falls back to portable SQL.
"""

from __future__ import annotations

from datasight.schema import _quote_identifier


def empty_string_preview(table: str, column: str, dialect: str) -> str:
    """Show rows where the column is the empty string, candidates for NULLIF."""
    qt = _quote_identifier(table)
    qc = _quote_identifier(column)
    return (
        f"-- Rows where {column!r} is an empty string. "
        f"To fix: UPDATE {qt} SET {qc} = NULL WHERE {qc} = '';\n"
        f"SELECT * FROM {qt} WHERE {qc} = '';"
    )


def whitespace_preview(table: str, column: str, dialect: str) -> str:
    """Show rows whose column value has surrounding whitespace."""
    qt = _quote_identifier(table)
    qc = _quote_identifier(column)
    return (
        f"-- Rows where {column!r} has leading/trailing whitespace. "
        f"To fix: UPDATE {qt} SET {qc} = TRIM({qc}) WHERE {qc} <> TRIM({qc});\n"
        f"SELECT {qc} AS original, TRIM({qc}) AS trimmed FROM {qt} "
        f"WHERE {qc} IS NOT NULL AND {qc} <> TRIM({qc});"
    )


def whole_row_dedup_preview(table: str, dialect: str) -> str:
    """Preview a deduplicated copy of the table."""
    qt = _quote_identifier(table)
    if dialect == "duckdb":
        materialize = (
            f"-- To materialize: CREATE OR REPLACE TABLE {qt} AS SELECT DISTINCT * FROM {qt};"
        )
    else:
        materialize = (
            f"-- To materialize: BEGIN; DROP TABLE IF EXISTS {qt}_deduped; "
            f"CREATE TABLE {qt}_deduped AS SELECT DISTINCT * FROM {qt}; COMMIT;"
        )
    return f"{materialize}\nSELECT DISTINCT * FROM {qt};"


def pk_dedup_preview(table: str, pk_column: str, dialect: str) -> str:
    """Show one canonical row per duplicate PK value.

    DuckDB uses ``QUALIFY`` for a one-liner; Postgres uses a CTE with
    ``ROW_NUMBER``; SQLite falls back to ``MIN(rowid)``.
    """
    qt = _quote_identifier(table)
    qc = _quote_identifier(pk_column)
    if dialect == "duckdb":
        return (
            f"-- One canonical row per duplicate {pk_column!r} value.\n"
            f"SELECT * FROM {qt} "
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {qc} ORDER BY {qc}) = 1;"
        )
    if dialect == "postgres":
        return (
            f"-- One canonical row per duplicate {pk_column!r} value.\n"
            f"WITH ranked AS (\n"
            f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY {qc} ORDER BY {qc}) AS rn "
            f"FROM {qt}\n"
            f") SELECT * FROM ranked WHERE rn = 1;"
        )
    # sqlite
    return (
        f"-- One canonical row per duplicate {pk_column!r} value (SQLite uses rowid).\n"
        f"SELECT * FROM {qt} WHERE rowid IN "
        f"(SELECT MIN(rowid) FROM {qt} GROUP BY {qc});"
    )


def outlier_preview(table: str, column: str, q1: str | None, q3: str | None, dialect: str) -> str:
    """Show rows whose value falls outside the IQR fence.

    ``q1`` / ``q3`` are stringified scalars from the original detector
    query so we can inline them as literals instead of re-running the
    percentile aggregate.
    """
    qt = _quote_identifier(table)
    qc = _quote_identifier(column)
    if q1 is None or q3 is None:
        return (
            f"-- Inspect outliers in {column!r} (recompute IQR fence as needed).\n"
            f"SELECT * FROM {qt} WHERE {qc} IS NOT NULL ORDER BY {qc} DESC LIMIT 20;"
        )
    return (
        f"-- Rows in {column!r} outside the IQR fence [q1={q1}, q3={q3}].\n"
        f"SELECT * FROM {qt} WHERE {qc} IS NOT NULL "
        f"AND ({qc} < {q1} - 1.5 * ({q3} - {q1}) "
        f"OR {qc} > {q3} + 1.5 * ({q3} - {q1}));"
    )


def orphan_fk_preview(
    table: str,
    column: str,
    parent_table: str,
    parent_column: str,
    dialect: str,
) -> str:
    """Show distinct child values not present in the parent's PK column."""
    qt = _quote_identifier(table)
    qc = _quote_identifier(column)
    qpt = _quote_identifier(parent_table)
    qpc = _quote_identifier(parent_column)
    return (
        f"-- Distinct {table}.{column} values not present in {parent_table}.{parent_column}.\n"
        f"SELECT DISTINCT {qc} FROM {qt} "
        f"WHERE {qc} IS NOT NULL "
        f"AND {qc} NOT IN (SELECT {qpc} FROM {qpt} WHERE {qpc} IS NOT NULL);"
    )
