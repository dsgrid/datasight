"""Cross-table referential integrity checks."""

from __future__ import annotations

from typing import Any

from loguru import logger

from datasight.data_profile import _looks_like_identifier, _quote_identifier, _run_scalar
from datasight.runner import RunSql


def _find_parent_table(
    column_name: str,
    all_table_names: dict[str, str],
) -> str | None:
    """Guess the parent table for a foreign-key column.

    Strips the ``_id`` suffix and checks for an exact or plural match
    among the known table names.
    """
    lower = column_name.lower()
    if not lower.endswith("_id"):
        return None
    base = lower[:-3]
    if base in all_table_names:
        return all_table_names[base]
    plural = base + "s"
    if plural in all_table_names:
        return all_table_names[plural]
    return None


async def _infer_primary_key(
    table_info: dict[str, Any],
    run_sql: RunSql,
) -> list[dict[str, Any]]:
    """Return candidate primary-key columns for a table."""
    table_name = table_info["name"]
    row_count = table_info.get("row_count") or 0
    if row_count == 0:
        return []

    candidates: list[dict[str, Any]] = []
    qt = _quote_identifier(table_name)

    for column in table_info.get("columns", []):
        col_name = column["name"]
        lower = col_name.lower()
        # Only consider columns that look like identifiers
        if not _looks_like_identifier(col_name):
            continue
        # Skip foreign keys (e.g. product_id in orders) — we want the table's own PK
        if lower != "id" and not lower.startswith(table_name.lower().rstrip("s") + "_"):
            # It's an FK to another table, not this table's own PK
            if lower.endswith("_id") and lower != "id":
                continue

        qc = _quote_identifier(col_name)
        distinct_count = await _run_scalar(
            run_sql,
            f"SELECT COUNT(DISTINCT {qc}) AS value FROM {qt}",
            "value",
        )
        if distinct_count is None:
            continue
        distinct_count = int(distinct_count)
        candidates.append(
            {
                "table": table_name,
                "column": col_name,
                "distinct_count": distinct_count,
                "row_count": row_count,
                "is_unique": distinct_count == row_count,
            }
        )

    return candidates


async def _check_orphans(
    child_table: str,
    child_column: str,
    parent_table: str,
    parent_column: str,
    run_sql: RunSql,
) -> dict[str, Any] | None:
    """Count child rows whose FK value has no match in the parent table."""
    qt_child = _quote_identifier(child_table)
    qc_child = _quote_identifier(child_column)
    qt_parent = _quote_identifier(parent_table)
    qc_parent = _quote_identifier(parent_column)

    orphan_sql = (
        f"SELECT COUNT(*) AS value FROM {qt_child} "
        f"WHERE {qc_child} IS NOT NULL "
        f"AND {qc_child} NOT IN (SELECT {qc_parent} FROM {qt_parent})"
    )
    child_count_sql = f"SELECT COUNT(*) AS value FROM {qt_child} WHERE {qc_child} IS NOT NULL"

    try:
        orphan_count = await _run_scalar(run_sql, orphan_sql, "value")
        child_rows = await _run_scalar(run_sql, child_count_sql, "value")
    except Exception as exc:
        logger.debug(f"Orphan check failed for {child_table}.{child_column}: {exc}")
        return None

    if orphan_count is None:
        return None

    orphan_count = int(orphan_count)
    child_rows = int(child_rows or 0)

    return {
        "child_table": child_table,
        "child_column": child_column,
        "parent_table": parent_table,
        "parent_column": parent_column,
        "orphan_count": orphan_count,
        "child_rows": child_rows,
    }


async def _check_join_explosion(
    table_a: str,
    fk_column: str,
    table_b: str,
    pk_column: str,
    run_sql: RunSql,
) -> dict[str, Any] | None:
    """Check whether joining two tables via a FK causes row multiplication."""
    qt_a = _quote_identifier(table_a)
    qc_fk = _quote_identifier(fk_column)
    qt_b = _quote_identifier(table_b)
    qc_pk = _quote_identifier(pk_column)

    join_sql = f"SELECT COUNT(*) AS value FROM {qt_a} a JOIN {qt_b} b ON a.{qc_fk} = b.{qc_pk}"
    base_sql = f"SELECT COUNT(*) AS value FROM {qt_a}"

    try:
        join_count = await _run_scalar(run_sql, join_sql, "value")
        base_count = await _run_scalar(run_sql, base_sql, "value")
    except Exception as exc:
        logger.debug(f"Join explosion check failed for {table_a}.{fk_column}: {exc}")
        return None

    if join_count is None or base_count is None:
        return None

    join_count = int(join_count)
    base_count = int(base_count)
    if base_count == 0:
        return None

    factor = round(join_count / base_count, 2)
    if factor <= 1.0:
        return None

    return {
        "table_a": table_a,
        "table_b": table_b,
        "join_column": fk_column,
        "expected_rows": base_count,
        "actual_rows": join_count,
        "explosion_factor": factor,
    }


async def build_integrity_overview(
    schema_info: list[dict[str, Any]],
    run_sql: RunSql,
    declared_joins: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic overview of referential integrity.

    Infers primary keys, detects duplicate keys, orphan foreign keys,
    and join explosion risks across all tables.
    """
    all_table_names: dict[str, str] = {t["name"].lower(): t["name"] for t in schema_info}
    table_lookup: dict[str, dict[str, Any]] = {t["name"].lower(): t for t in schema_info}

    primary_keys: list[dict[str, Any]] = []
    duplicate_keys: list[dict[str, Any]] = []
    orphan_foreign_keys: list[dict[str, Any]] = []
    join_explosions: list[dict[str, Any]] = []
    notes: list[str] = []

    # --- Infer primary keys ---
    pk_map: dict[str, str] = {}  # table_lower -> pk column name
    for table in schema_info:
        candidates = await _infer_primary_key(table, run_sql)
        for candidate in candidates:
            primary_keys.append(candidate)
            if candidate["is_unique"]:
                pk_map[table["name"].lower()] = candidate["column"]
            else:
                duplicate_keys.append(
                    {
                        "table": candidate["table"],
                        "column": candidate["column"],
                        "duplicate_count": candidate["row_count"] - candidate["distinct_count"],
                    }
                )

    # --- Check foreign keys ---
    relationships: list[
        tuple[str, str, str, str]
    ] = []  # child_table, child_col, parent_table, parent_pk

    if declared_joins:
        for join in declared_joins:
            relationships.append(
                (
                    join["child_table"],
                    join["child_column"],
                    join["parent_table"],
                    join.get("parent_column", "id"),
                )
            )
    else:
        # Infer FK relationships from column names
        for table in schema_info:
            for column in table.get("columns", []):
                col_name = column["name"]
                if not _looks_like_identifier(col_name) or col_name.lower() == "id":
                    continue
                # Skip the table's own PK
                if pk_map.get(table["name"].lower()) == col_name:
                    continue
                parent = _find_parent_table(col_name, all_table_names)
                if parent is None:
                    continue
                parent_pk = pk_map.get(parent.lower(), "id")
                # Verify the parent has that PK column
                parent_info = table_lookup.get(parent.lower())
                if parent_info is None:
                    continue
                parent_cols = {c["name"].lower() for c in parent_info.get("columns", [])}
                if parent_pk.lower() not in parent_cols:
                    continue
                relationships.append((table["name"], col_name, parent, parent_pk))

    for child_table, child_col, parent_table, parent_pk in relationships[:8]:
        orphan_result = await _check_orphans(
            child_table, child_col, parent_table, parent_pk, run_sql
        )
        if orphan_result and orphan_result["orphan_count"] > 0:
            orphan_foreign_keys.append(orphan_result)

        explosion = await _check_join_explosion(
            child_table, child_col, parent_table, parent_pk, run_sql
        )
        if explosion:
            join_explosions.append(explosion)

    # --- Notes ---
    if not primary_keys:
        notes.append("No obvious primary key columns detected.")
    if not relationships:
        notes.append("No foreign-key relationships inferred between tables.")
    if not orphan_foreign_keys and relationships:
        notes.append("No orphan foreign keys detected — all references resolve.")
    if not join_explosions and relationships:
        notes.append("No join explosion risks detected — all joins are safe.")
    if not duplicate_keys:
        notes.append("All inferred primary keys are unique.")

    return {
        "table_count": len(schema_info),
        "primary_keys": primary_keys[:8],
        "duplicate_keys": duplicate_keys[:8],
        "orphan_foreign_keys": orphan_foreign_keys[:8],
        "join_explosions": join_explosions[:8],
        "notes": notes,
    }
