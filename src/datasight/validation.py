"""Declarative data validation rules engine."""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import yaml
from loguru import logger

from datasight.data_profile import _quote_identifier, _run_scalar
from datasight.runner import RunSql


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------


def load_validation_config(
    path: str | None,
    project_dir: str,
) -> list[dict[str, Any]]:
    """Load validation rules from YAML.

    Expected format is a list of mappings, each with ``table`` and ``rules``.
    """
    if not path:
        default = os.path.join(project_dir, "validation.yaml")
        if os.path.exists(default):
            path = default
        else:
            return []
    if not os.path.exists(path):
        logger.warning(f"Validation config not found: {path}")
        return []
    with open(path, encoding="utf-8") as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.warning(f"Failed to parse {path}: {e}")
            return []
    if not isinstance(data, list):
        logger.warning(f"Expected a list in {path}, got {type(data).__name__}")
        return []
    return data


# ---------------------------------------------------------------------------
# Rule handlers
# ---------------------------------------------------------------------------


def _result(
    table: str,
    rule_type: str,
    column: str | None,
    status: str,
    detail: str,
    value: Any = None,
) -> dict[str, Any]:
    return {
        "table": table,
        "rule": rule_type,
        "column": column,
        "status": status,
        "detail": detail,
        "value": value,
    }


async def _check_required_columns(
    table_info: dict[str, Any],
    rule: dict[str, Any],
    run_sql: RunSql,  # noqa: ARG001
) -> dict[str, Any]:
    table = table_info["name"]
    required = set(rule.get("columns") or [])
    actual = {c["name"] for c in table_info.get("columns", [])}
    missing = required - actual
    if missing:
        return _result(
            table,
            "required_columns",
            None,
            "fail",
            f"Missing columns: {', '.join(sorted(missing))}",
            sorted(missing),
        )
    return _result(table, "required_columns", None, "pass", "All required columns present.")


async def _check_max_null_rate(
    table_info: dict[str, Any],
    rule: dict[str, Any],
    run_sql: RunSql,
) -> dict[str, Any]:
    table = table_info["name"]
    column = rule.get("column", "")
    threshold = float(rule.get("threshold", 0.05))
    qt = _quote_identifier(table)
    qc = _quote_identifier(column)
    sql = (
        f"SELECT SUM(CASE WHEN {qc} IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS value FROM {qt}"
    )
    rate = await _run_scalar(run_sql, sql, "value")
    if rate is None:
        return _result(table, "max_null_rate", column, "warn", "Could not compute null rate.")
    rate = float(rate)
    if rate > threshold:
        return _result(
            table,
            "max_null_rate",
            column,
            "fail",
            f"Null rate {rate:.1%} exceeds threshold {threshold:.1%}.",
            round(rate, 4),
        )
    return _result(
        table,
        "max_null_rate",
        column,
        "pass",
        f"Null rate {rate:.1%} within threshold {threshold:.1%}.",
        round(rate, 4),
    )


async def _check_numeric_range(
    table_info: dict[str, Any],
    rule: dict[str, Any],
    run_sql: RunSql,
) -> dict[str, Any]:
    table = table_info["name"]
    column = rule.get("column", "")
    qt = _quote_identifier(table)
    qc = _quote_identifier(column)
    sql = f"SELECT MIN({qc}) AS min_val, MAX({qc}) AS max_val FROM {qt} WHERE {qc} IS NOT NULL"
    try:
        df = await run_sql(sql)
    except Exception as exc:
        return _result(table, "numeric_range", column, "warn", f"Query failed: {exc}")
    if df.empty:
        return _result(table, "numeric_range", column, "warn", "No data.")

    min_val = df.iloc[0].get("min_val")
    max_val = df.iloc[0].get("max_val")
    rule_min = rule.get("min")
    rule_max = rule.get("max")
    issues: list[str] = []

    if rule_min is not None and min_val is not None and float(min_val) < float(rule_min):
        issues.append(f"min value {min_val} < allowed minimum {rule_min}")
    if rule_max is not None and max_val is not None and float(max_val) > float(rule_max):
        issues.append(f"max value {max_val} > allowed maximum {rule_max}")

    if issues:
        return _result(
            table,
            "numeric_range",
            column,
            "fail",
            "; ".join(issues),
            {"actual_min": min_val, "actual_max": max_val},
        )
    return _result(
        table,
        "numeric_range",
        column,
        "pass",
        f"Values in range [{rule_min}, {rule_max}].",
        {"actual_min": min_val, "actual_max": max_val},
    )


async def _check_allowed_values(
    table_info: dict[str, Any],
    rule: dict[str, Any],
    run_sql: RunSql,
) -> dict[str, Any]:
    table = table_info["name"]
    column = rule.get("column", "")
    allowed = set(str(v) for v in (rule.get("values") or []))
    qt = _quote_identifier(table)
    qc = _quote_identifier(column)
    sql = f"SELECT DISTINCT {qc} AS value FROM {qt} WHERE {qc} IS NOT NULL"
    try:
        df = await run_sql(sql)
    except Exception as exc:
        return _result(table, "allowed_values", column, "warn", f"Query failed: {exc}")

    actual = {str(v) for v in df["value"].tolist()} if not df.empty else set()
    unexpected = actual - allowed
    if unexpected:
        sample = sorted(unexpected)[:5]
        return _result(
            table,
            "allowed_values",
            column,
            "fail",
            f"{len(unexpected)} unexpected values: {', '.join(sample)}",
            sorted(unexpected),
        )
    return _result(table, "allowed_values", column, "pass", "All values in allowed set.")


async def _check_regex(
    table_info: dict[str, Any],
    rule: dict[str, Any],
    run_sql: RunSql,
) -> dict[str, Any]:
    table = table_info["name"]
    column = rule.get("column", "")
    pattern = rule.get("pattern", "")
    qt = _quote_identifier(table)
    qc = _quote_identifier(column)

    # Try DuckDB regexp_matches first
    sql = (
        f"SELECT COUNT(*) AS value FROM {qt} "
        f"WHERE {qc} IS NOT NULL AND NOT regexp_matches({qc}, '{pattern}')"
    )
    try:
        violation_count = await _run_scalar(run_sql, sql, "value")
        violation_count = int(violation_count or 0)
    except Exception:
        # Fallback: fetch values and check in Python
        fetch_sql = f"SELECT {qc} AS value FROM {qt} WHERE {qc} IS NOT NULL"
        try:
            df = await run_sql(fetch_sql)
            compiled = re.compile(pattern)
            violation_count = sum(1 for v in df["value"].tolist() if not compiled.search(str(v)))
        except Exception as exc:
            return _result(table, "regex", column, "warn", f"Regex check failed: {exc}")

    if violation_count > 0:
        return _result(
            table,
            "regex",
            column,
            "fail",
            f"{violation_count} values do not match pattern '{pattern}'.",
            violation_count,
        )
    return _result(table, "regex", column, "pass", f"All values match pattern '{pattern}'.")


async def _check_uniqueness(
    table_info: dict[str, Any],
    rule: dict[str, Any],
    run_sql: RunSql,
) -> dict[str, Any]:
    table = table_info["name"]
    columns = rule.get("columns") or []
    if not columns:
        return _result(table, "uniqueness", None, "warn", "No columns specified.")

    qt = _quote_identifier(table)
    quoted_cols = ", ".join(_quote_identifier(c) for c in columns)
    col_label = ", ".join(columns)

    sql = (
        f"SELECT COUNT(*) AS value FROM ("
        f"SELECT {quoted_cols} FROM {qt} "
        f"GROUP BY {quoted_cols} HAVING COUNT(*) > 1"
        f")"
    )
    try:
        dup_groups = await _run_scalar(run_sql, sql, "value")
    except Exception as exc:
        return _result(table, "uniqueness", col_label, "warn", f"Query failed: {exc}")

    dup_groups = int(dup_groups or 0)
    if dup_groups > 0:
        return _result(
            table,
            "uniqueness",
            col_label,
            "fail",
            f"{dup_groups} duplicate groups on ({col_label}).",
            dup_groups,
        )
    return _result(table, "uniqueness", col_label, "pass", f"({col_label}) is unique.")


async def _check_monotonic(
    table_info: dict[str, Any],
    rule: dict[str, Any],
    run_sql: RunSql,
) -> dict[str, Any]:
    table = table_info["name"]
    column = rule.get("column", "")
    direction = rule.get("direction", "non_decreasing")
    qt = _quote_identifier(table)
    qc = _quote_identifier(column)

    if direction == "increasing":
        cond = f"t.{qc} <= t.prev_val"
    else:  # non_decreasing
        cond = f"t.{qc} < t.prev_val"

    sql = (
        f"SELECT COUNT(*) AS value FROM ("
        f"SELECT {qc}, LAG({qc}) OVER (ORDER BY {qc}) AS prev_val FROM {qt}"
        f") t WHERE t.prev_val IS NOT NULL AND {cond}"
    )
    try:
        violations = await _run_scalar(run_sql, sql, "value")
    except Exception as exc:
        return _result(table, "monotonic", column, "warn", f"Query failed: {exc}")

    violations = int(violations or 0)
    if violations > 0:
        return _result(
            table,
            "monotonic",
            column,
            "fail",
            f"{violations} {direction} monotonicity violations.",
            violations,
        )
    return _result(table, "monotonic", column, "pass", f"Column is {direction}.")


async def _check_row_count(
    table_info: dict[str, Any],
    rule: dict[str, Any],
    run_sql: RunSql,  # noqa: ARG001
) -> dict[str, Any]:
    table = table_info["name"]
    row_count = table_info.get("row_count") or 0
    rule_min = rule.get("min")
    rule_max = rule.get("max")
    issues: list[str] = []

    if rule_min is not None and row_count < int(rule_min):
        issues.append(f"row count {row_count} < minimum {rule_min}")
    if rule_max is not None and row_count > int(rule_max):
        issues.append(f"row count {row_count} > maximum {rule_max}")

    if issues:
        return _result(table, "row_count", None, "fail", "; ".join(issues), row_count)
    return _result(
        table, "row_count", None, "pass", f"Row count {row_count} within range.", row_count
    )


async def _check_freshness(
    table_info: dict[str, Any],
    rule: dict[str, Any],
    run_sql: RunSql,
) -> dict[str, Any]:
    table = table_info["name"]
    column = rule.get("column", "")
    max_age_days = int(rule.get("max_age_days", 90))
    qt = _quote_identifier(table)
    qc = _quote_identifier(column)

    sql = f"SELECT MAX({qc}) AS value FROM {qt}"
    max_date = await _run_scalar(run_sql, sql, "value")
    if max_date is None:
        return _result(table, "freshness", column, "warn", "No date values found.")

    try:
        if isinstance(max_date, str):
            # Parse common date formats
            for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    parsed = datetime.strptime(max_date, fmt).replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    continue
            else:
                return _result(
                    table, "freshness", column, "warn", f"Could not parse date: {max_date}"
                )
        else:
            # pandas Timestamp or datetime
            parsed = datetime.fromisoformat(str(max_date)).replace(tzinfo=timezone.utc)
    except Exception:
        return _result(table, "freshness", column, "warn", f"Could not parse date: {max_date}")

    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    age_days = (datetime.now(timezone.utc) - parsed).days

    if parsed < cutoff:
        return _result(
            table,
            "freshness",
            column,
            "fail",
            f"Most recent date is {max_date} ({age_days} days old), exceeds {max_age_days}-day SLA.",
            age_days,
        )
    return _result(
        table,
        "freshness",
        column,
        "pass",
        f"Most recent date is {max_date} ({age_days} days old), within {max_age_days}-day SLA.",
        age_days,
    )


# ---------------------------------------------------------------------------
# Rule dispatcher
# ---------------------------------------------------------------------------

_RULE_HANDLERS: dict[str, Any] = {
    "required_columns": _check_required_columns,
    "max_null_rate": _check_max_null_rate,
    "numeric_range": _check_numeric_range,
    "allowed_values": _check_allowed_values,
    "regex": _check_regex,
    "uniqueness": _check_uniqueness,
    "monotonic": _check_monotonic,
    "row_count": _check_row_count,
    "freshness": _check_freshness,
}


# ---------------------------------------------------------------------------
# Public builder
# ---------------------------------------------------------------------------


async def build_validation_report(
    schema_info: list[dict[str, Any]],
    run_sql: RunSql,
    rules: list[dict[str, Any]],
) -> dict[str, Any]:
    """Run declarative validation rules against the database.

    Parameters
    ----------
    schema_info:
        Schema information (list of table dicts with name, row_count, columns).
    run_sql:
        Async SQL execution callable.
    rules:
        List of rule group dicts, each with ``table`` and ``rules`` keys.
    """
    table_lookup: dict[str, dict[str, Any]] = {t["name"].lower(): t for t in schema_info}
    results: list[dict[str, Any]] = []
    rule_count = 0
    tables_seen: set[str] = set()

    for group in rules:
        table_name = group.get("table", "")
        table_info = table_lookup.get(table_name.lower())
        if table_info is None:
            results.append(
                _result(table_name, "table_exists", None, "fail", f"Table not found: {table_name}")
            )
            continue

        tables_seen.add(table_info["name"])
        for rule in group.get("rules", []):
            rule_type = rule.get("type", "")
            handler = _RULE_HANDLERS.get(rule_type)
            if handler is None:
                results.append(
                    _result(
                        table_info["name"],
                        rule_type,
                        None,
                        "warn",
                        f"Unknown rule type: {rule_type}",
                    )
                )
                continue
            rule_count += 1
            result = await handler(table_info, rule, run_sql)
            results.append(result)

    summary = {
        "pass": sum(1 for r in results if r["status"] == "pass"),
        "fail": sum(1 for r in results if r["status"] == "fail"),
        "warn": sum(1 for r in results if r["status"] == "warn"),
    }

    return {
        "table_count": len(tables_seen),
        "rule_count": rule_count,
        "results": results,
        "summary": summary,
    }
