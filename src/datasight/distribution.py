"""Distribution profiling for numeric columns."""

from __future__ import annotations

from typing import Any

from loguru import logger

from datasight.data_profile import (
    _infer_measure_semantics,
    _is_date_dtype,
    _is_numeric_dtype,
    _looks_like_identifier,
    _quote_identifier,
    _run_scalar,
)
from datasight.runner import RunSql


async def _compute_percentiles(
    run_sql: RunSql,
    table: str,
    column: str,
) -> dict[str, float | None]:
    """Compute percentiles for a numeric column.

    Tries ``PERCENTILE_CONT`` (DuckDB / Postgres) first and falls back
    to ``ORDER BY … LIMIT 1 OFFSET n`` for SQLite.
    """
    qt = _quote_identifier(table)
    qc = _quote_identifier(column)

    # DuckDB / Postgres path
    sql = (
        f"SELECT "
        f"PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY {qc}) AS p1, "
        f"PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY {qc}) AS p5, "
        f"PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {qc}) AS q1, "
        f"PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {qc}) AS p50, "
        f"PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {qc}) AS q3, "
        f"PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {qc}) AS p95, "
        f"PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY {qc}) AS p99 "
        f"FROM {qt} WHERE {qc} IS NOT NULL"
    )

    try:
        df = await run_sql(sql)
        if not df.empty:
            row = df.iloc[0]
            result: dict[str, float | None] = {}
            for key in ("p1", "p5", "q1", "p50", "q3", "p95", "p99"):
                v = row.get(key)
                if v is None:
                    result[key] = None
                    continue
                try:
                    f = float(v)
                except (TypeError, ValueError):
                    result[key] = None
                    continue
                result[key] = None if f != f else f  # NaN check
            return result
    except Exception:
        pass

    # SQLite fallback: ORDER BY + LIMIT/OFFSET
    count_val = await _run_scalar(
        run_sql,
        f"SELECT COUNT(*) AS value FROM {qt} WHERE {qc} IS NOT NULL",
        "value",
    )
    if not count_val or int(count_val) == 0:
        return {k: None for k in ("p1", "p5", "q1", "p50", "q3", "p95", "p99")}

    total = int(count_val)
    result = {}
    for label, quantile in [
        ("p1", 0.01),
        ("p5", 0.05),
        ("q1", 0.25),
        ("p50", 0.50),
        ("q3", 0.75),
        ("p95", 0.95),
        ("p99", 0.99),
    ]:
        offset = min(int(total * quantile), total - 1)
        val = await _run_scalar(
            run_sql,
            f"SELECT {qc} AS value FROM {qt} WHERE {qc} IS NOT NULL ORDER BY {qc} LIMIT 1 OFFSET {offset}",
            "value",
        )
        if val is None:
            result[label] = None
            continue
        try:
            f = float(val)
        except (TypeError, ValueError):
            result[label] = None
            continue
        result[label] = None if f != f else f  # NaN check
    return result


async def _compute_stats(
    run_sql: RunSql,
    table: str,
    column: str,
) -> dict[str, float | None]:
    """Compute basic distribution stats: total, zero/negative counts, mean, stddev."""
    qt = _quote_identifier(table)
    qc = _quote_identifier(column)

    sql = (
        f"SELECT "
        f"COUNT(*) AS total, "
        f"SUM(CASE WHEN {qc} = 0 THEN 1 ELSE 0 END) AS zero_count, "
        f"SUM(CASE WHEN {qc} < 0 THEN 1 ELSE 0 END) AS negative_count, "
        f"AVG({qc} * 1.0) AS mean_val, "
        f"SQRT(AVG({qc} * 1.0 * {qc} * 1.0) - AVG({qc} * 1.0) * AVG({qc} * 1.0)) AS stddev_val "
        f"FROM {qt} WHERE {qc} IS NOT NULL"
    )

    try:
        df = await run_sql(sql)
    except Exception as exc:
        logger.debug(f"Stats query failed for {table}.{column}: {exc}")
        return {}

    if df.empty:
        return {}

    row = df.iloc[0]

    def _safe_int(key: str) -> int:
        v = row.get(key)
        if v is None:
            return 0
        try:
            f = float(v)
        except (TypeError, ValueError):
            return 0
        if f != f:  # NaN check
            return 0
        return int(f)

    def _safe_float(key: str) -> float | None:
        v = row.get(key)
        if v is None:
            return None
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        if f != f:  # NaN check
            return None
        return f

    return {
        "total": _safe_int("total"),
        "zero_count": _safe_int("zero_count"),
        "negative_count": _safe_int("negative_count"),
        "mean": _safe_float("mean_val"),
        "stddev": _safe_float("stddev_val"),
    }


async def _count_iqr_outliers(
    run_sql: RunSql,
    table: str,
    column: str,
    q1: float,
    q3: float,
) -> int:
    """Count values outside the 1.5 * IQR fences."""
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    qt = _quote_identifier(table)
    qc = _quote_identifier(column)
    sql = (
        f"SELECT COUNT(*) AS value FROM {qt} "
        f"WHERE {qc} IS NOT NULL AND ({qc} < {lower} OR {qc} > {upper})"
    )
    val = await _run_scalar(run_sql, sql, "value")
    return int(val) if val is not None else 0


def _check_energy_flags(
    table: str,
    column: str,
    semantics: dict[str, Any] | None,
    pctiles: dict[str, float | None],
    stats: dict[str, float | None],
) -> list[dict[str, Any]]:
    """Return energy-domain flags based on measure semantics and distribution."""
    if not semantics:
        return []

    flags: list[dict[str, Any]] = []
    role = semantics.get("role", "")
    unit = semantics.get("unit") or ""
    negative_count = stats.get("negative_count", 0) or 0
    total = stats.get("total", 0) or 0

    if role == "energy" and negative_count > 0:
        rate = round(negative_count / total * 100, 1) if total else 0
        flags.append(
            {
                "table": table,
                "column": column,
                "flag": "negative_generation",
                "detail": (
                    f"Energy column has {negative_count} negative values ({rate}%). "
                    "Negative generation is usually a data error."
                ),
            }
        )

    p99 = pctiles.get("p99")
    if role == "ratio" and p99 is not None:
        if p99 > 100:
            flags.append(
                {
                    "table": table,
                    "column": column,
                    "flag": "capacity_factor_over_100pct",
                    "detail": f"Ratio column has p99={p99:.2f}, suggesting values expressed as percentages exceeding 100%.",
                }
            )
        elif p99 > 1.0:
            flags.append(
                {
                    "table": table,
                    "column": column,
                    "flag": "capacity_factor_over_1",
                    "detail": f"Ratio column has p99={p99:.4f}, exceeding 1.0 (capacity factor > 100%).",
                }
            )

    p1 = pctiles.get("p1")
    if role == "rate" and "mmbtu" in unit:
        if (p1 is not None and p1 < 3) or (p99 is not None and p99 > 30):
            flags.append(
                {
                    "table": table,
                    "column": column,
                    "flag": "implausible_heat_rate",
                    "detail": (
                        f"Heat rate outside plausible range (3–30 MMBtu/MWh): p1={p1}, p99={p99}."
                    ),
                }
            )

    # Zero-denominator risk for rates and ratios
    if role in {"rate", "ratio", "price"} and stats.get("zero_count", 0):
        zero_count = stats["zero_count"]
        flags.append(
            {
                "table": table,
                "column": column,
                "flag": "zero_values_in_rate",
                "detail": (
                    f"Rate/ratio column has {zero_count} zero values which may indicate "
                    "zero-denominator issues in upstream calculations."
                ),
            }
        )

    return flags


async def _detect_spikes(
    run_sql: RunSql,
    table: str,
    date_column: str,
    measure_column: str,
) -> list[dict[str, Any]]:
    """Detect per-month aggregation spikes (> 3 sigma from mean)."""
    qt = _quote_identifier(table)
    qd = _quote_identifier(date_column)
    qm = _quote_identifier(measure_column)

    # Try DATE_TRUNC first (DuckDB / Postgres), then STRFTIME (SQLite)
    for period_expr in (
        f"DATE_TRUNC('month', {qd})",
        f"STRFTIME('%Y-%m', {qd})",
    ):
        sql = (
            f"SELECT {period_expr} AS period, AVG({qm}) AS period_avg "
            f"FROM {qt} WHERE {qd} IS NOT NULL AND {qm} IS NOT NULL "
            f"GROUP BY 1 ORDER BY 1"
        )
        try:
            df = await run_sql(sql)
            if df.empty or "period_avg" not in df.columns:
                return []
            break
        except Exception:
            continue
    else:
        return []

    values = [float(v) for v in df["period_avg"].tolist() if v is not None]
    if len(values) < 4:
        return []

    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    stddev = variance**0.5
    if stddev == 0:
        return []

    spikes: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        period_avg = row.get("period_avg")
        if period_avg is None:
            continue
        z = abs(float(period_avg) - mean) / stddev
        if z > 3.0:
            spikes.append(
                {
                    "table": table,
                    "measure_column": measure_column,
                    "period": str(row["period"]),
                    "period_avg": round(float(period_avg), 4),
                    "z_score": round(z, 2),
                    "detail": (
                        f"{measure_column} avg={float(period_avg):.4f} in period "
                        f"{row['period']} is {z:.1f} standard deviations from the mean ({mean:.4f})."
                    ),
                }
            )
    return spikes[:8]


async def build_distribution_overview(
    schema_info: list[dict[str, Any]],
    run_sql: RunSql,
    overrides: list[dict[str, Any]] | None = None,
    target_column: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic distribution profile for numeric columns.

    For each numeric non-identifier column, computes percentiles,
    zero/negative rates, IQR outliers, and energy-specific flags.
    """
    distributions: list[dict[str, Any]] = []
    energy_flags: list[dict[str, Any]] = []
    spikes: list[dict[str, Any]] = []
    notes: list[str] = []
    saw_energy_semantics = False

    # Build override map for measure semantics
    override_map: dict[tuple[str, str], dict[str, Any]] = {}
    for item in overrides or []:
        key = (
            str(item.get("table") or "").lower(),
            str(item.get("column") or "").lower(),
        )
        override_map[key] = item

    for table in schema_info:
        table_name = table["name"]
        sibling_columns = [c["name"] for c in table.get("columns", [])]
        date_column: str | None = None

        # Find a date column in this table for spike detection
        for column in table.get("columns", []):
            if _is_date_dtype(column.get("dtype", "")):
                date_column = column["name"]
                break

        for column in table.get("columns", []):
            col_name = column["name"]
            dtype = column.get("dtype", "")

            if not _is_numeric_dtype(dtype) or _looks_like_identifier(col_name):
                continue

            if target_column:
                target_parts = target_column.split(".", 1)
                if len(target_parts) == 2:
                    if (
                        table_name.lower() != target_parts[0].lower()
                        or col_name.lower() != target_parts[1].lower()
                    ):
                        continue
                else:
                    continue

            if len(distributions) >= 8 and not target_column:
                break

            # Compute percentiles and stats
            pctiles = await _compute_percentiles(run_sql, table_name, col_name)
            stats = await _compute_stats(run_sql, table_name, col_name)

            total = stats.get("total", 0) or 0
            zero_count = stats.get("zero_count", 0) or 0
            negative_count = stats.get("negative_count", 0) or 0
            mean = stats.get("mean")
            stddev = stats.get("stddev")

            q1 = pctiles.get("q1")
            q3 = pctiles.get("q3")
            outlier_count = 0
            iqr: float | None = None
            if q1 is not None and q3 is not None:
                iqr = round(q3 - q1, 4)
                outlier_count = await _count_iqr_outliers(run_sql, table_name, col_name, q1, q3)

            cv: float | None = None
            if mean and stddev and mean != 0:
                cv = round(abs(stddev / mean), 4)

            dist_entry = {
                "table": table_name,
                "column": col_name,
                "p1": pctiles.get("p1"),
                "p5": pctiles.get("p5"),
                "p50": pctiles.get("p50"),
                "p95": pctiles.get("p95"),
                "p99": pctiles.get("p99"),
                "q1": q1,
                "q3": q3,
                "iqr": iqr,
                "outlier_count": outlier_count,
                "zero_rate": round(zero_count / total * 100, 1) if total else None,
                "negative_rate": round(negative_count / total * 100, 1) if total else None,
                "mean": round(mean, 4) if mean is not None else None,
                "stddev": round(stddev, 4) if stddev is not None else None,
                "cv": cv,
            }

            # Infer measure semantics for energy flags
            semantics = _infer_measure_semantics(col_name, dtype, sibling_columns)
            if semantics:
                dist_entry["role"] = semantics.get("role")
                dist_entry["unit"] = semantics.get("unit")
                # Only count the generic-measure role as non-energy; domain roles
                # (energy, ratio, rate, price, power, capacity) justify the
                # "no anomalies detected" note.
                if semantics.get("role") and semantics["role"] != "measure":
                    saw_energy_semantics = True
                column_flags = _check_energy_flags(table_name, col_name, semantics, pctiles, stats)
                energy_flags.extend(column_flags)

            distributions.append(dist_entry)

            # Spike detection
            if date_column:
                column_spikes = await _detect_spikes(run_sql, table_name, date_column, col_name)
                spikes.extend(column_spikes)

        if len(distributions) >= 8 and not target_column:
            break

    if not distributions:
        notes.append("No numeric non-identifier columns found for distribution profiling.")
    if saw_energy_semantics and not energy_flags:
        notes.append("No energy-specific anomalies detected.")
    if not spikes:
        notes.append("No temporal spikes detected in per-period aggregations.")

    return {
        "table_count": len(schema_info),
        "distributions": distributions[:8] if not target_column else distributions,
        "energy_flags": energy_flags[:8],
        "spikes": spikes[:8],
        "notes": notes,
    }
