"""Deterministic dataset profiling helpers for UI and CLI flows."""

from __future__ import annotations

from typing import Any

import yaml
from loguru import logger

from datasight.runner import RunSql


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _is_date_dtype(dtype: str) -> bool:
    lower = dtype.lower()
    return any(token in lower for token in ("date", "time", "timestamp"))


def _is_numeric_dtype(dtype: str) -> bool:
    lower = dtype.lower()
    return any(
        token in lower
        for token in ("int", "decimal", "numeric", "float", "double", "real", "number")
    )


def _is_text_dtype(dtype: str) -> bool:
    lower = dtype.lower()
    return any(token in lower for token in ("char", "text", "string", "varchar"))


def _looks_like_identifier(name: str) -> bool:
    lower = name.lower()
    return lower == "id" or lower.endswith("_id") or lower.startswith("id_")


def _looks_like_count(name: str) -> bool:
    lower = name.lower()
    return any(
        token in lower
        for token in (
            "count",
            "customers",
            "customer_count",
            "meters",
            "units",
            "plants",
            "outages",
            "events",
            "transactions",
            "records",
        )
    )


def _extract_unit(name: str) -> str | None:
    lower = name.lower()
    for unit in (
        "mwh",
        "kwh",
        "gwh",
        "twh",
        "mw",
        "kw",
        "gw",
        "mmbtu",
        "btu",
        "usd_per_mwh",
        "cents_per_kwh",
        "lb_per_mwh",
        "kg_per_mwh",
        "pct",
    ):
        if unit in lower:
            return unit
    if "percent" in lower:
        return "percent"
    return None


def _name_tokens(name: str) -> set[str]:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in name)
    return {part for part in cleaned.split("_") if part}


def _suggest_weight_column(
    column_name: str,
    role: str,
    unit: str | None,
    sibling_columns: list[str],
) -> str | None:
    lower = column_name.lower()
    preferred: list[str] = []

    if role == "price":
        preferred.extend(
            [
                "net_generation_mwh",
                "generation_mwh",
                "load_mwh",
                "energy_mwh",
                "fuel_consumed_mmbtu",
                "fuel_mmbtu",
                "quantity",
            ]
        )
    elif role == "rate":
        if "mmbtu" in lower or unit == "mmbtu":
            preferred.extend(["fuel_consumed_mmbtu", "fuel_mmbtu"])
        preferred.extend(
            [
                "net_generation_mwh",
                "generation_mwh",
                "load_mwh",
                "energy_mwh",
                "output_mwh",
            ]
        )
    elif role == "ratio":
        preferred.extend(
            [
                "net_generation_mwh",
                "generation_mwh",
                "load_mwh",
                "energy_mwh",
                "customers",
                "customer_count",
            ]
        )

    lower_siblings = {name.lower(): name for name in sibling_columns if name.lower() != lower}
    for candidate in preferred:
        match = lower_siblings.get(candidate.lower())
        if match:
            return match
    return None


def _recommended_rollup_sql(
    measure_sql: str,
    default_aggregation: str,
    weight_column: str | None,
    alias_name: str,
) -> str:
    weighted_expr = measure_sql if measure_sql == alias_name else f"({measure_sql})"
    if default_aggregation == "avg" and weight_column:
        return (
            f"SUM({weighted_expr} * {weight_column}) / NULLIF(SUM({weight_column}), 0) "
            f"AS weighted_avg_{alias_name}"
        )
    if default_aggregation == "sum":
        return f"SUM({measure_sql}) AS total_{alias_name}"
    if default_aggregation == "max":
        return f"MAX({measure_sql}) AS peak_{alias_name}"
    if default_aggregation == "min":
        return f"MIN({measure_sql}) AS min_{alias_name}"
    return f"AVG({measure_sql}) AS avg_{alias_name}"


def _measure_sql_expression(item: dict[str, Any]) -> str:
    expression = str(item.get("expression") or "").strip()
    if expression:
        return expression
    return str(item.get("column") or "")


def _measure_alias_name(item: dict[str, Any]) -> str:
    return str(item.get("column") or item.get("name") or "measure")


def _recommended_chart_type(item: dict[str, Any], default: str = "line") -> str:
    preferred = item.get("preferred_chart_types") or []
    if preferred:
        return str(preferred[0])
    role = str(item.get("measure_role") or item.get("role") or "")
    fmt = str(item.get("measure_format") or item.get("format") or "")
    aggregation = str(item.get("aggregation") or item.get("default_aggregation") or "")
    if fmt == "percent" or role in {"ratio"}:
        return "line"
    if role in {"energy", "count"} and aggregation == "sum":
        return "area"
    if role in {"power", "capacity"}:
        return "line"
    return default


def _build_override_measure(
    table_name: str,
    override: dict[str, Any],
    sibling_columns: list[str],
) -> dict[str, Any]:
    measure_name = str(override.get("column") or override.get("name") or "").strip()
    inferred = _infer_measure_semantics(measure_name, "DOUBLE", sibling_columns) or {
        "column": measure_name,
        "dtype": "DOUBLE",
        "role": "measure",
        "unit": None,
        "default_aggregation": "avg",
        "average_strategy": "avg",
        "weight_column": None,
        "allowed_aggregations": ["avg", "min", "max"],
        "forbidden_aggregations": [],
        "additive_across_category": False,
        "additive_across_time": False,
        "confidence": 0.5,
        "reason": "Project-defined semantic measure.",
        "recommended_rollup_sql": "",
    }
    inferred["column"] = measure_name
    if override.get("name"):
        inferred["name"] = str(override["name"])
    if override.get("expression"):
        inferred["expression"] = str(override["expression"])
        inferred["source"] = "calculated"
        inferred["reason"] = str(override.get("reason") or "Project-defined calculated measure.")
        inferred["confidence"] = 1.0
    return {"table": table_name, **_apply_measure_override(inferred, override)}


def _apply_measure_override(
    inferred: dict[str, Any],
    override: dict[str, Any] | None,
) -> dict[str, Any]:
    if not override:
        return inferred

    merged = dict(inferred)
    for key in (
        "role",
        "unit",
        "default_aggregation",
        "average_strategy",
        "weight_column",
        "reason",
        "description",
        "display_name",
        "format",
        "allowed_aggregations",
        "forbidden_aggregations",
        "additive_across_category",
        "additive_across_time",
    ):
        if key in override:
            merged[key] = override[key]
    if "preferred_chart_types" in override:
        merged["preferred_chart_types"] = override["preferred_chart_types"]
    if "name" in override:
        merged["name"] = override["name"]
    if "expression" in override:
        merged["expression"] = override["expression"]
        merged["source"] = "calculated"

    merged["recommended_rollup_sql"] = _recommended_rollup_sql(
        _measure_sql_expression(merged),
        str(merged.get("default_aggregation") or "avg"),
        merged.get("weight_column"),
        _measure_alias_name(merged),
    )
    return merged


def _infer_measure_semantics(
    column_name: str,
    dtype: str,
    sibling_columns: list[str] | None = None,
) -> dict[str, Any] | None:
    lower = column_name.lower()
    tokens = _name_tokens(column_name)
    if not _is_numeric_dtype(dtype) or _looks_like_identifier(column_name):
        return None
    sibling_columns = sibling_columns or []

    unit = _extract_unit(column_name)
    role = "measure"
    default_aggregation = "avg"
    allowed_aggregations = ["avg", "min", "max"]
    forbidden_aggregations: list[str] = []
    average_strategy = "avg"
    weight_column: str | None = None
    additive_across_category = False
    additive_across_time = False
    confidence = 0.55
    reason = "Numeric column with no strong unit or aggregation hints."

    price_tokens = ("price", "price_", "_price", "cost_per", "usd_per", "cents_per", "lmp")
    rate_tokens = ("heat_rate", "emissions_rate", "intensity", "rate", "lb_per_", "kg_per_")
    capacity_tokens = ("capacity", "nameplate", "capability")
    energy_tokens = (
        "generation",
        "consumption",
        "energy",
        "mwh",
        "kwh",
        "gwh",
        "twh",
        "mmbtu",
        "fuel_burn",
        "fuel_use",
    )

    if any(token in tokens for token in ("pct", "percent", "share", "ratio", "factor")) or any(
        phrase in lower for phrase in ("capacity_factor", "load_factor")
    ):
        role = "ratio"
        default_aggregation = "avg"
        allowed_aggregations = ["avg", "min", "max"]
        forbidden_aggregations = ["sum"]
        reason = (
            "Ratio or factor metric; averaging or taking extrema is usually safer than summing."
        )
        confidence = 0.95 if unit or "factor" in lower else 0.85
    elif any(token in lower for token in price_tokens):
        role = "price"
        default_aggregation = "avg"
        allowed_aggregations = ["avg", "min", "max"]
        forbidden_aggregations = ["sum"]
        reason = (
            "Price-like metric; average or extrema usually make sense, while sums usually do not."
        )
        confidence = 0.93 if unit else 0.84
    elif any(token in lower for token in rate_tokens):
        role = "rate"
        default_aggregation = "avg"
        allowed_aggregations = ["avg", "min", "max"]
        forbidden_aggregations = ["sum"]
        reason = "Rate or intensity metric; average over time or groups instead of summing."
        confidence = 0.9 if unit else 0.8
    elif any(token in lower for token in capacity_tokens):
        role = "capacity"
        default_aggregation = "max" if "summer" in lower or "winter" in lower else "avg"
        allowed_aggregations = ["avg", "max", "min"]
        forbidden_aggregations = ["sum"]
        additive_across_category = True
        reason = "Capacity-like metric; summarize with averages or peaks, not sums over time."
        confidence = 0.94 if unit else 0.86
    elif any(token in tokens for token in ("demand", "load", "power")) or unit in {
        "mw",
        "kw",
        "gw",
    }:
        role = "power"
        default_aggregation = "max" if "peak" in lower else "avg"
        allowed_aggregations = ["avg", "max", "min"]
        forbidden_aggregations = ["sum"]
        additive_across_category = True
        reason = "Power or demand metric; average or peak over time rather than summing."
        confidence = 0.95 if unit in {"mw", "kw", "gw"} else 0.84
    elif any(token in lower for token in energy_tokens):
        role = "energy"
        default_aggregation = "sum"
        allowed_aggregations = ["sum", "avg", "min", "max"]
        additive_across_category = True
        additive_across_time = True
        reason = (
            "Energy-volume metric; summing across periods and categories is usually meaningful."
        )
        confidence = 0.96 if unit in {"mwh", "kwh", "gwh", "twh", "mmbtu", "btu"} else 0.85
    elif _looks_like_count(column_name):
        role = "count"
        default_aggregation = "sum"
        allowed_aggregations = ["sum", "avg", "min", "max"]
        additive_across_category = True
        additive_across_time = True
        reason = "Count-like metric; totals are usually the primary roll-up."
        confidence = 0.82
    elif any(token in lower for token in ("total", "net_", "gross_", "volume")):
        default_aggregation = "sum"
        allowed_aggregations = ["sum", "avg", "min", "max"]
        additive_across_category = True
        additive_across_time = True
        reason = "Name suggests an accumulated quantity, so totals are a reasonable default."
        confidence = 0.7

    if role in {"price", "rate", "ratio"}:
        weight_column = _suggest_weight_column(column_name, role, unit, sibling_columns)
        if weight_column:
            average_strategy = "weighted_avg"
            reason += f" Prefer a weighted average using `{weight_column}` when rolling up."

    recommended_rollup_sql = _recommended_rollup_sql(
        column_name,
        default_aggregation,
        weight_column,
        column_name,
    )

    return {
        "column": column_name,
        "dtype": dtype,
        "role": role,
        "unit": unit,
        "default_aggregation": default_aggregation,
        "average_strategy": average_strategy,
        "weight_column": weight_column,
        "allowed_aggregations": allowed_aggregations,
        "forbidden_aggregations": forbidden_aggregations,
        "additive_across_category": additive_across_category,
        "additive_across_time": additive_across_time,
        "confidence": round(confidence, 2),
        "reason": reason,
        "recommended_rollup_sql": recommended_rollup_sql,
    }


async def _run_scalar(run_sql: RunSql, sql: str, column: str) -> Any:
    try:
        df = await run_sql(sql)
    except Exception as exc:
        logger.debug(f"Profile query failed: {exc}")
        return None

    if df.empty or column not in df.columns:
        return None
    return df.iloc[0][column]


async def _get_date_coverage(run_sql: RunSql, table: str, column: str) -> dict[str, Any] | None:
    quoted_table = _quote_identifier(table)
    quoted_column = _quote_identifier(column)
    sql = (
        f"SELECT MIN({quoted_column}) AS min_value, MAX({quoted_column}) AS max_value "
        f"FROM {quoted_table} WHERE {quoted_column} IS NOT NULL"
    )

    try:
        df = await run_sql(sql)
    except Exception as exc:
        logger.debug(f"Date coverage query failed for {table}.{column}: {exc}")
        return None

    if df.empty:
        return None

    min_value = df.iloc[0].get("min_value")
    max_value = df.iloc[0].get("max_value")
    if min_value is None and max_value is None:
        return None

    return {
        "table": table,
        "column": column,
        "min": None if min_value is None else str(min_value),
        "max": None if max_value is None else str(max_value),
    }


async def _get_dimension_stats(
    run_sql: RunSql,
    table: str,
    column: str,
    row_count: int | None,
) -> dict[str, Any] | None:
    quoted_table = _quote_identifier(table)
    quoted_column = _quote_identifier(column)
    stats_sql = (
        f"SELECT COUNT(DISTINCT {quoted_column}) AS distinct_count, "
        f"SUM(CASE WHEN {quoted_column} IS NULL THEN 1 ELSE 0 END) AS null_count "
        f"FROM {quoted_table}"
    )
    sample_sql = (
        f"SELECT {quoted_column} AS value FROM {quoted_table} "
        f"WHERE {quoted_column} IS NOT NULL "
        f"GROUP BY 1 ORDER BY COUNT(*) DESC, 1 LIMIT 3"
    )

    try:
        stats_df = await run_sql(stats_sql)
        sample_df = await run_sql(sample_sql)
    except Exception as exc:
        logger.debug(f"Dimension stats query failed for {table}.{column}: {exc}")
        return None

    if stats_df.empty:
        return None

    distinct_count = stats_df.iloc[0].get("distinct_count")
    null_count = stats_df.iloc[0].get("null_count")
    null_rate = None
    if row_count:
        try:
            null_rate = round((float(null_count or 0) / row_count) * 100, 1)
        except (TypeError, ValueError, ZeroDivisionError):
            null_rate = None

    samples = []
    if not sample_df.empty and "value" in sample_df.columns:
        samples = [str(v) for v in sample_df["value"].tolist() if v is not None]

    return {
        "table": table,
        "column": column,
        "distinct_count": None if distinct_count is None else int(distinct_count),
        "null_count": None if null_count is None else int(null_count),
        "null_rate": null_rate,
        "sample_values": samples,
    }


async def build_dataset_overview(
    schema_info: list[dict[str, Any]], run_sql: RunSql
) -> dict[str, Any]:
    """Build a deterministic overview of the loaded dataset."""
    total_rows = sum((table.get("row_count") or 0) for table in schema_info)
    total_columns = sum(len(table.get("columns", [])) for table in schema_info)

    tables = sorted(
        schema_info,
        key=lambda item: ((item.get("row_count") or 0), item.get("name", "")),
        reverse=True,
    )

    largest_tables = [
        {
            "name": table["name"],
            "row_count": table.get("row_count"),
            "column_count": len(table.get("columns", [])),
        }
        for table in tables[:5]
    ]

    date_candidates: list[tuple[str, str]] = []
    measure_candidates: list[dict[str, Any]] = []
    dimension_candidates: list[tuple[str, str, int | None]] = []

    for table in schema_info:
        table_name = table["name"]
        row_count = table.get("row_count")
        for column in table.get("columns", []):
            column_name = column["name"]
            dtype = column.get("dtype", "")
            if _is_date_dtype(dtype):
                date_candidates.append((table_name, column_name))
            elif _is_numeric_dtype(dtype) and not _looks_like_identifier(column_name):
                measure_candidates.append(
                    {"table": table_name, "column": column_name, "dtype": dtype}
                )
            elif _is_text_dtype(dtype) and not _looks_like_identifier(column_name):
                dimension_candidates.append((table_name, column_name, row_count))

    date_coverages: list[dict[str, Any]] = []
    for table_name, column_name in date_candidates[:6]:
        coverage = await _get_date_coverage(run_sql, table_name, column_name)
        if coverage:
            date_coverages.append(coverage)

    dimension_stats: list[dict[str, Any]] = []
    for table_name, column_name, row_count in dimension_candidates[:6]:
        stats = await _get_dimension_stats(run_sql, table_name, column_name, row_count)
        if stats:
            dimension_stats.append(stats)

    quality_flags: list[str] = []
    if not date_candidates:
        quality_flags.append("No obvious date columns detected.")
    if not measure_candidates:
        quality_flags.append("No obvious numeric measure columns detected.")
    if len(schema_info) == 1:
        quality_flags.append("Single-table dataset: start with profiling or category breakdowns.")

    return {
        "table_count": len(schema_info),
        "total_rows": total_rows,
        "total_columns": total_columns,
        "largest_tables": largest_tables,
        "date_columns": date_coverages,
        "measure_columns": measure_candidates[:8],
        "dimension_columns": dimension_stats,
        "quality_flags": quality_flags,
    }


async def build_dimension_overview(
    schema_info: list[dict[str, Any]],
    run_sql: RunSql,
) -> dict[str, Any]:
    """Build a deterministic overview focused on grouping dimensions."""
    overview = await build_dataset_overview(schema_info, run_sql)
    dimension_columns = overview.get("dimension_columns", [])
    date_columns = overview.get("date_columns", [])
    measure_columns = overview.get("measure_columns", [])

    best_dimensions = sorted(
        dimension_columns,
        key=lambda item: (
            -abs((item.get("distinct_count") or 0) - 12),
            item.get("null_rate") or 0,
            item.get("table", ""),
            item.get("column", ""),
        ),
    )[:8]

    suggested_breakdowns: list[dict[str, Any]] = []
    for item in best_dimensions[:5]:
        sample_values = item.get("sample_values") or []
        suggested_breakdowns.append(
            {
                "table": item["table"],
                "column": item["column"],
                "reason": (
                    f"{item.get('distinct_count') or '?'} distinct values"
                    + (f", samples: {', '.join(sample_values[:3])}" if sample_values else "")
                ),
            }
        )

    join_hints: list[str] = []
    for table in schema_info:
        for column in table.get("columns", []):
            column_name = column["name"]
            if _looks_like_identifier(column_name) and column_name.lower() != "id":
                base = column_name[:-3] if column_name.lower().endswith("_id") else column_name
                for other in schema_info:
                    if other["name"] == table["name"]:
                        continue
                    if (
                        other["name"].lower() == base.lower()
                        or other["name"].lower() == f"{base.lower()}s"
                    ):
                        join_hints.append(
                            f"{table['name']}.{column_name} likely joins to {other['name']}.id"
                        )
                        break

    if not join_hints and len(schema_info) == 1:
        join_hints.append("Single-table dataset: start with category and time breakdowns.")

    return {
        "table_count": overview["table_count"],
        "dimension_columns": best_dimensions,
        "date_columns": date_columns[:5],
        "measure_columns": measure_columns[:5],
        "suggested_breakdowns": suggested_breakdowns,
        "join_hints": join_hints[:6],
    }


async def build_measure_overview(
    schema_info: list[dict[str, Any]],
    run_sql: RunSql,  # noqa: ARG001
    overrides: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic overview of likely measures and aggregations."""
    measures: list[dict[str, Any]] = []
    override_map = {
        (str(item.get("table") or "").lower(), str(item.get("column") or "").lower()): item
        for item in (overrides or [])
    }

    for table in schema_info:
        table_name = table["name"]
        sibling_columns = [str(column["name"]) for column in table.get("columns", [])]
        for column in table.get("columns", []):
            override = override_map.get((table_name.lower(), str(column["name"]).lower()))
            inferred = _infer_measure_semantics(
                column["name"],
                column.get("dtype", ""),
                sibling_columns,
            )
            if inferred is None and override and _is_numeric_dtype(column.get("dtype", "")):
                inferred = {
                    "column": column["name"],
                    "dtype": column.get("dtype", ""),
                    "role": "measure",
                    "unit": None,
                    "default_aggregation": "avg",
                    "average_strategy": "avg",
                    "weight_column": None,
                    "allowed_aggregations": ["avg", "min", "max"],
                    "forbidden_aggregations": [],
                    "additive_across_category": False,
                    "additive_across_time": False,
                    "confidence": 0.5,
                    "reason": "Project override applied to a numeric column.",
                    "recommended_rollup_sql": _recommended_rollup_sql(
                        column["name"], "avg", None, column["name"]
                    ),
                }
            if inferred is None:
                continue
            measures.append(
                {
                    "table": table_name,
                    **_apply_measure_override(inferred, override),
                }
            )

        for override in overrides or []:
            if str(override.get("table") or "").lower() != table_name.lower():
                continue
            if not override.get("expression") or not override.get("name"):
                continue
            measures.append(_build_override_measure(table_name, override, sibling_columns))

    measures.sort(
        key=lambda item: (
            -float(item.get("confidence") or 0),
            item.get("table", ""),
            item.get("column", ""),
        )
    )

    notes: list[str] = []
    roles = {str(item.get("role")) for item in measures}
    if "energy" in roles:
        notes.append("Energy-volume fields (for example MWh) usually roll up with SUM.")
    if "power" in roles:
        notes.append("Power and demand fields (for example MW) usually need AVG or MAX, not SUM.")
    if "capacity" in roles:
        notes.append(
            "Capacity fields are usually meaningful as installed levels or peaks, not period totals."
        )
    if {"rate", "price", "ratio"} & roles:
        notes.append(
            "Rates, prices, and factors should usually be averaged or otherwise weighted, not summed."
        )
    if any(item.get("average_strategy") == "weighted_avg" for item in measures):
        notes.append(
            "When an obvious denominator exists, weighted averages are safer than plain averages for rates and prices."
        )
    if not measures:
        notes.append("No obvious numeric measures detected.")

    return {
        "table_count": len(schema_info),
        "measures": measures[:16],
        "notes": notes,
    }


def format_measure_overrides_yaml(measure_data: dict[str, Any]) -> str:
    """Render inferred measures as an editable measures.yaml scaffold."""
    entries: list[dict[str, Any]] = []
    for item in measure_data.get("measures", []):
        entry: dict[str, Any] = {
            "table": item["table"],
            "column": item.get("column"),
            "name": item.get("name"),
            "expression": item.get("expression"),
            "role": item.get("role", "measure"),
            "display_name": item.get("display_name"),
            "format": item.get("format"),
            "unit": item.get("unit"),
            "default_aggregation": item.get("default_aggregation", "avg"),
            "average_strategy": item.get("average_strategy", "avg"),
            "weight_column": item.get("weight_column"),
            "preferred_chart_types": item.get("preferred_chart_types", []),
            "allowed_aggregations": item.get("allowed_aggregations", []),
            "forbidden_aggregations": item.get("forbidden_aggregations", []),
            "additive_across_category": bool(item.get("additive_across_category")),
            "additive_across_time": bool(item.get("additive_across_time")),
            "reason": item.get("reason", ""),
        }
        entries.append({key: value for key, value in entry.items() if value not in (None, [], "")})

    header = [
        "# datasight measure overrides",
        "# Edit these entries to lock in project-specific aggregation behavior.",
        "# Fields omitted here will keep using inferred defaults.",
        "",
    ]
    body = yaml.safe_dump(entries, sort_keys=False, allow_unicode=False).strip()
    return "\n".join(header) + body + "\n"


def format_measure_prompt_context(measure_data: dict[str, Any]) -> str:
    """Render inferred measure semantics as prompt guidance."""
    measures = measure_data.get("measures") or []
    if not measures:
        return ""

    lines = [
        "\n## Inferred Measure Semantics",
        "Use these aggregation defaults unless the user explicitly asks for something else.",
        "Do not SUM prices, rates, percentages, or factors unless the user explicitly requests that behavior.",
        "For power-like signals (for example MW load or demand), prefer AVG or MAX over time rather than SUM.",
        "When a measure includes a weight column below, prefer a weighted average instead of a plain AVG for rollups.",
    ]
    for item in measures[:8]:
        unit = f", unit={item['unit']}" if item.get("unit") else ""
        avoid = (
            f", avoid={', '.join(item['forbidden_aggregations'])}"
            if item.get("forbidden_aggregations")
            else ""
        )
        weight = (
            f", weight={item['weight_column']}, average=weighted_avg"
            if item.get("weight_column")
            else ""
        )
        display_name = f", display_name={item['display_name']}" if item.get("display_name") else ""
        fmt = f", format={item['format']}" if item.get("format") else ""
        charts = (
            f", preferred_charts={', '.join(item['preferred_chart_types'])}"
            if item.get("preferred_chart_types")
            else ""
        )
        expression = f", expression={item['expression']}" if item.get("expression") else ""
        rollup_sql = item.get("recommended_rollup_sql") or _recommended_rollup_sql(
            _measure_sql_expression(item),
            str(item.get("default_aggregation") or "avg"),
            item.get("weight_column"),
            _measure_alias_name(item),
        )
        formula = f", rollup_sql={rollup_sql}"
        lines.append(
            "- "
            f"{item['table']}.{item['column']}: role={item['role']}, "
            f"default={item['default_aggregation']}, "
            f"allowed={', '.join(item['allowed_aggregations'])}"
            f"{avoid}{unit}{weight}{display_name}{fmt}{charts}{expression}{formula}. {item['reason']}"
        )
    return "\n".join(lines) + "\n"


async def build_quality_overview(
    schema_info: list[dict[str, Any]],
    run_sql: RunSql,
) -> dict[str, Any]:
    """Build a deterministic overview focused on data quality signals."""
    null_columns: list[dict[str, Any]] = []
    numeric_flags: list[dict[str, Any]] = []
    date_columns: list[dict[str, Any]] = []
    notes: list[str] = []

    for table in schema_info:
        table_name = table["name"]
        row_count = table.get("row_count")
        for column in table.get("columns", []):
            column_name = column["name"]
            dtype = column.get("dtype", "")

            null_count = await _run_scalar(
                run_sql,
                (
                    f"SELECT SUM(CASE WHEN {_quote_identifier(column_name)} IS NULL THEN 1 ELSE 0 END) "
                    f"AS value FROM {_quote_identifier(table_name)}"
                ),
                "value",
            )
            if null_count and row_count:
                try:
                    null_rate = round((float(null_count or 0) / row_count) * 100, 1)
                except (TypeError, ValueError, ZeroDivisionError):
                    null_rate = None
                if null_rate and null_rate >= 10:
                    null_columns.append(
                        {
                            "table": table_name,
                            "column": column_name,
                            "null_count": int(null_count),
                            "null_rate": null_rate,
                        }
                    )

            if _is_numeric_dtype(dtype) and not _looks_like_identifier(column_name):
                stats = await _get_numeric_stats(run_sql, table_name, column_name)
                if stats:
                    min_value = stats.get("min")
                    max_value = stats.get("max")
                    avg_value = stats.get("avg")
                    if min_value == max_value and min_value is not None:
                        numeric_flags.append(
                            {
                                "table": table_name,
                                "column": column_name,
                                "issue": f"constant numeric value ({min_value})",
                            }
                        )
                    elif avg_value in {min_value, max_value} and min_value != max_value:
                        numeric_flags.append(
                            {
                                "table": table_name,
                                "column": column_name,
                                "issue": f"average sits on boundary ({avg_value})",
                            }
                        )
            elif _is_date_dtype(dtype):
                coverage = await _get_date_coverage(run_sql, table_name, column_name)
                if coverage:
                    date_columns.append(coverage)

    if not null_columns:
        notes.append("No null-heavy columns detected in the sampled profiling pass.")
    if not date_columns:
        notes.append("No obvious date columns detected for freshness checks.")
    if not numeric_flags:
        notes.append("No obviously degenerate numeric ranges detected.")

    return {
        "table_count": len(schema_info),
        "null_columns": sorted(
            null_columns,
            key=lambda item: (item.get("null_rate") or 0, item["table"], item["column"]),
            reverse=True,
        )[:8],
        "numeric_flags": numeric_flags[:8],
        "date_columns": date_columns[:6],
        "notes": notes,
    }


async def build_trend_overview(
    schema_info: list[dict[str, Any]],
    run_sql: RunSql,
    overrides: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic overview of likely time-series analyses."""
    overview = await build_dataset_overview(schema_info, run_sql)
    measure_overview = await build_measure_overview(schema_info, run_sql, overrides)
    date_columns = overview.get("date_columns", [])
    semantic_measures = measure_overview.get("measures", [])
    dimension_columns = overview.get("dimension_columns", [])

    trend_candidates: list[dict[str, Any]] = []
    for date_item in date_columns[:6]:
        same_table_measures = [
            measure
            for measure in semantic_measures
            if measure.get("table") == date_item.get("table")
        ]
        if not same_table_measures:
            same_table_measures = semantic_measures[:3]
        for measure in same_table_measures[:3]:
            aggregation = str(measure.get("default_aggregation") or "sum")
            if aggregation == "avg" and measure.get("average_strategy") == "weighted_avg":
                aggregation = "weighted_avg"
            trend_candidates.append(
                {
                    "table": date_item["table"],
                    "date_column": date_item["column"],
                    "measure_column": measure["column"],
                    "measure_expression": measure.get("expression"),
                    "measure_dtype": measure.get("dtype", ""),
                    "measure_role": measure.get("role", ""),
                    "measure_display_name": measure.get("display_name"),
                    "measure_format": measure.get("format"),
                    "preferred_chart_types": measure.get("preferred_chart_types", []),
                    "aggregation": aggregation,
                    "weight_column": measure.get("weight_column"),
                    "date_range": (
                        f"{date_item.get('min') or '?'} → {date_item.get('max') or '?'}"
                    ),
                    "recommended_query_shape": (
                        f"WEIGHTED_AVG({measure['column']} BY {measure['weight_column']}) "
                        f"BY {date_item['column']}"
                        if aggregation == "weighted_avg" and measure.get("weight_column")
                        else f"{aggregation.upper()}({measure['column']}) BY {date_item['column']}"
                    ),
                    "recommended_rollup_sql": measure.get("recommended_rollup_sql"),
                }
            )

    trend_candidates = trend_candidates[:8]

    breakout_dimensions = [
        item
        for item in dimension_columns
        if item.get("distinct_count") is not None and (item.get("distinct_count") or 0) <= 24
    ][:6]

    chart_recommendations: list[dict[str, Any]] = []
    for candidate in trend_candidates[:4]:
        aggregation = str(candidate.get("aggregation") or "sum")
        role = str(candidate.get("measure_role") or "measure")
        chart_type = _recommended_chart_type(candidate, "line")
        chart_recommendations.append(
            {
                "title": (
                    f"{aggregation.upper()} "
                    f"{candidate.get('measure_display_name') or candidate['measure_column']} "
                    f"over {candidate['date_column']}"
                ),
                "table": candidate["table"],
                "chart_type": chart_type,
                "preferred_chart_types": candidate.get("preferred_chart_types", []),
                "aggregation": aggregation,
                "reason": (
                    f"{role} metric with default {aggregation.upper()} aggregation"
                    + (
                        f", format `{candidate['measure_format']}`"
                        if candidate.get("measure_format")
                        else ""
                    )
                    + (
                        f" using `{candidate['weight_column']}` as the weight"
                        if aggregation == "weighted_avg" and candidate.get("weight_column")
                        else ""
                    )
                    + "; "
                    f"date coverage {candidate['date_range']}"
                ),
            }
        )

    notes: list[str] = []
    if not date_columns:
        notes.append("No obvious date columns detected for time-series analysis.")
    if not semantic_measures:
        notes.append("No obvious numeric measures detected for charting.")
    if date_columns and semantic_measures and not breakout_dimensions:
        notes.append("Start with a single-series line chart before adding category splits.")

    return {
        "table_count": overview["table_count"],
        "trend_candidates": trend_candidates,
        "breakout_dimensions": breakout_dimensions,
        "chart_recommendations": chart_recommendations,
        "notes": notes,
    }


async def build_prompt_recipes(
    schema_info: list[dict[str, Any]],
    run_sql: RunSql,
    overrides: list[dict[str, Any]] | None = None,
) -> list[dict[str, str]]:
    """Build reusable prompt recipes from deterministic schema overviews."""
    dataset_overview = await build_dataset_overview(schema_info, run_sql)
    dimension_overview = await build_dimension_overview(schema_info, run_sql)
    measure_overview = await build_measure_overview(schema_info, run_sql, overrides)
    trend_overview = await build_trend_overview(schema_info, run_sql, overrides)

    recipes: list[dict[str, str]] = [
        {
            "title": "Profile the biggest tables",
            "category": "Orientation",
            "reason": "Starts with the highest-row-count tables so you can orient yourself quickly.",
            "prompt": (
                "Profile the biggest and most important tables in this dataset. "
                "Summarize what each table appears to represent, how large it is, and what columns matter most."
            ),
        },
        {
            "title": "Audit nulls and suspicious ranges",
            "category": "Quality",
            "reason": "Covers the fastest deterministic data quality checks before deeper analysis.",
            "prompt": (
                "Audit this dataset for null-heavy columns, suspicious numeric ranges, stale dates, "
                "and other obvious data quality issues. Organize findings by severity."
            ),
        },
    ]

    top_dimension = next(iter(dimension_overview.get("suggested_breakdowns", [])), None)
    if top_dimension:
        recipes.append(
            {
                "title": f"Break down by {top_dimension['column']}",
                "category": "Dimensions",
                "reason": (
                    f"Suggested because `{top_dimension['table']}.{top_dimension['column']}` "
                    "looked like a strong grouping dimension."
                ),
                "prompt": (
                    f"Analyze the most important breakdowns using `{top_dimension['table']}.{top_dimension['column']}`. "
                    f"Explain why this dimension matters, show the top categories, and recommend follow-up cuts."
                ),
            }
        )

    top_trend = next(iter(trend_overview.get("trend_candidates", [])), None)
    if top_trend:
        recipes.append(
            {
                "title": f"Trend {top_trend['measure_column']} over {top_trend['date_column']}",
                "category": "Trend",
                "reason": (
                    f"Suggested from the detected date/measure pair on `{top_trend['table']}` "
                    f"covering {top_trend['date_range']}."
                ),
                "prompt": (
                    f"Build a time-series analysis for `{top_trend['table']}` using `{top_trend['date_column']}` "
                    f"and `{top_trend['measure_column']}`. "
                    + (
                        f"Use this rollup shape as a guide: `{top_trend['recommended_rollup_sql']}`. "
                        if top_trend.get("recommended_rollup_sql")
                        else ""
                    )
                    + f"Create a chart and explain the main pattern in the range {top_trend['date_range']}."
                ),
            }
        )

    top_measure = next(iter(dataset_overview.get("measure_columns", [])), None)
    if top_measure:
        recipes.append(
            {
                "title": f"Summarize {top_measure['column']}",
                "category": "Measures",
                "reason": (
                    f"Suggested because `{top_measure['table']}.{top_measure['column']}` "
                    "looked like a central numeric measure."
                ),
                "prompt": (
                    f"Summarize the key behavior of `{top_measure['table']}.{top_measure['column']}`. "
                    "Show totals, distribution shape, major segments, and any anomalies worth investigating."
                ),
            }
        )

    semantic_measure = next(iter(measure_overview.get("measures", [])), None)
    if semantic_measure:
        aggregation = semantic_measure["default_aggregation"].upper()
        recipes.append(
            {
                "title": f"{aggregation} {semantic_measure['column']}",
                "category": "Measures",
                "reason": (
                    f"Suggested because `{semantic_measure['table']}.{semantic_measure['column']}` "
                    f"looks like a `{semantic_measure['role']}` measure with default `{semantic_measure['default_aggregation']}` aggregation."
                ),
                "prompt": (
                    f"Analyze `{semantic_measure['table']}.{semantic_measure['column']}` as a `{semantic_measure['role']}` measure. "
                    + (
                        f"It is a calculated measure defined as `{semantic_measure['expression']}`. "
                        if semantic_measure.get("expression")
                        else ""
                    )
                    + (
                        f"Start with its default `{semantic_measure['default_aggregation']}` aggregation, explain why that aggregation fits the metric, "
                        f"and use this SQL rollup shape as a guide: `{semantic_measure['recommended_rollup_sql']}`. "
                        "Show the most decision-useful breakdowns."
                    )
                ),
            }
        )

    power_measure = next(
        (
            item
            for item in measure_overview.get("measures", [])
            if item.get("role") in {"power", "capacity"}
        ),
        None,
    )
    if power_measure:
        recipes.append(
            {
                "title": f"Peak vs average {power_measure['column']}",
                "category": "Energy",
                "reason": (
                    f"Suggested because `{power_measure['table']}.{power_measure['column']}` "
                    "looks non-additive over time and is better summarized with averages or peaks."
                ),
                "prompt": (
                    f"Compare average and peak behavior for `{power_measure['table']}.{power_measure['column']}` over time. "
                    f"Use `{power_measure['recommended_rollup_sql']}` as one candidate rollup shape. "
                    "Explain when AVG is the right summary, when MAX is the right summary, and create the most useful chart."
                ),
            }
        )

    return recipes[:6]


def find_table_info(schema_info: list[dict[str, Any]], table_name: str) -> dict[str, Any] | None:
    """Return a schema table by name."""
    lower = table_name.lower()
    return next((table for table in schema_info if table["name"].lower() == lower), None)


def find_column_info(table_info: dict[str, Any], column_name: str) -> dict[str, Any] | None:
    """Return a column descriptor from a table schema entry."""
    lower = column_name.lower()
    return next(
        (column for column in table_info.get("columns", []) if column["name"].lower() == lower),
        None,
    )


async def build_table_profile(table_info: dict[str, Any], run_sql: RunSql) -> dict[str, Any]:
    """Build a deterministic profile for a single table."""
    table_name = table_info["name"]
    row_count = table_info.get("row_count")
    columns = table_info.get("columns", [])

    null_columns: list[dict[str, Any]] = []
    numeric_columns: list[dict[str, Any]] = []
    date_columns: list[dict[str, Any]] = []
    text_columns: list[dict[str, Any]] = []

    for column in columns:
        column_name = column["name"]
        dtype = column.get("dtype", "")

        null_count = await _run_scalar(
            run_sql,
            (
                f"SELECT SUM(CASE WHEN {_quote_identifier(column_name)} IS NULL THEN 1 ELSE 0 END) "
                f"AS value FROM {_quote_identifier(table_name)}"
            ),
            "value",
        )
        null_rate = None
        if row_count:
            try:
                null_rate = round((float(null_count or 0) / row_count) * 100, 1)
            except (TypeError, ValueError, ZeroDivisionError):
                null_rate = None
        if null_count:
            null_columns.append(
                {
                    "column": column_name,
                    "null_count": int(null_count),
                    "null_rate": null_rate,
                }
            )

        if _is_numeric_dtype(dtype):
            stats = await _get_numeric_stats(run_sql, table_name, column_name)
            if stats:
                numeric_columns.append(stats)
        elif _is_date_dtype(dtype):
            coverage = await _get_date_coverage(run_sql, table_name, column_name)
            if coverage:
                date_columns.append(coverage)
        elif _is_text_dtype(dtype):
            stats = await _get_dimension_stats(run_sql, table_name, column_name, row_count)
            if stats:
                text_columns.append(stats)

    return {
        "table": table_name,
        "row_count": row_count,
        "column_count": len(columns),
        "columns": columns,
        "null_columns": sorted(
            null_columns,
            key=lambda item: (item.get("null_rate") or 0, item["column"]),
            reverse=True,
        )[:8],
        "numeric_columns": numeric_columns[:8],
        "date_columns": date_columns[:8],
        "text_columns": text_columns[:8],
    }


async def build_column_profile(
    table_info: dict[str, Any],
    column_info: dict[str, Any],
    run_sql: RunSql,
) -> dict[str, Any]:
    """Build a deterministic profile for a single column."""
    table_name = table_info["name"]
    column_name = column_info["name"]
    dtype = column_info.get("dtype", "")
    row_count = table_info.get("row_count")

    profile: dict[str, Any] = {
        "table": table_name,
        "column": column_name,
        "dtype": dtype,
        "row_count": row_count,
    }

    null_count = await _run_scalar(
        run_sql,
        (
            f"SELECT SUM(CASE WHEN {_quote_identifier(column_name)} IS NULL THEN 1 ELSE 0 END) "
            f"AS value FROM {_quote_identifier(table_name)}"
        ),
        "value",
    )
    distinct_count = await _run_scalar(
        run_sql,
        (
            f"SELECT COUNT(DISTINCT {_quote_identifier(column_name)}) AS value "
            f"FROM {_quote_identifier(table_name)}"
        ),
        "value",
    )

    profile["null_count"] = None if null_count is None else int(null_count)
    profile["distinct_count"] = None if distinct_count is None else int(distinct_count)
    if row_count:
        try:
            profile["null_rate"] = round((float(null_count or 0) / row_count) * 100, 1)
        except (TypeError, ValueError, ZeroDivisionError):
            profile["null_rate"] = None
    else:
        profile["null_rate"] = None

    if _is_numeric_dtype(dtype):
        profile["numeric_stats"] = await _get_numeric_stats(run_sql, table_name, column_name)
    elif _is_date_dtype(dtype):
        profile["date_coverage"] = await _get_date_coverage(run_sql, table_name, column_name)
    elif _is_text_dtype(dtype):
        profile["dimension_stats"] = await _get_dimension_stats(
            run_sql, table_name, column_name, row_count
        )
    else:
        profile["sample_values"] = await _get_sample_values(run_sql, table_name, column_name)

    return profile


async def _get_numeric_stats(run_sql: RunSql, table: str, column: str) -> dict[str, Any] | None:
    quoted_table = _quote_identifier(table)
    quoted_column = _quote_identifier(column)
    sql = (
        f"SELECT MIN({quoted_column}) AS min_value, "
        f"MAX({quoted_column}) AS max_value, "
        f"AVG({quoted_column}) AS avg_value "
        f"FROM {quoted_table} WHERE {quoted_column} IS NOT NULL"
    )
    try:
        df = await run_sql(sql)
    except Exception as exc:
        logger.debug(f"Numeric stats query failed for {table}.{column}: {exc}")
        return None

    if df.empty:
        return None

    return {
        "table": table,
        "column": column,
        "min": None if df.iloc[0].get("min_value") is None else str(df.iloc[0]["min_value"]),
        "max": None if df.iloc[0].get("max_value") is None else str(df.iloc[0]["max_value"]),
        "avg": None if df.iloc[0].get("avg_value") is None else str(df.iloc[0]["avg_value"]),
    }


async def _get_sample_values(run_sql: RunSql, table: str, column: str) -> list[str]:
    quoted_table = _quote_identifier(table)
    quoted_column = _quote_identifier(column)
    sql = (
        f"SELECT {quoted_column} AS value FROM {quoted_table} "
        f"WHERE {quoted_column} IS NOT NULL "
        f"GROUP BY 1 ORDER BY COUNT(*) DESC, 1 LIMIT 5"
    )
    try:
        df = await run_sql(sql)
    except Exception as exc:
        logger.debug(f"Sample values query failed for {table}.{column}: {exc}")
        return []

    if df.empty or "value" not in df.columns:
        return []
    return [str(value) for value in df["value"].tolist() if value is not None]
