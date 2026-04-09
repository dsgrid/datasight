"""Deterministic dataset profiling helpers for UI and CLI flows."""

from __future__ import annotations

from typing import Any

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
) -> dict[str, Any]:
    """Build a deterministic overview of likely time-series analyses."""
    overview = await build_dataset_overview(schema_info, run_sql)
    date_columns = overview.get("date_columns", [])
    measure_columns = overview.get("measure_columns", [])
    dimension_columns = overview.get("dimension_columns", [])

    trend_candidates: list[dict[str, Any]] = []
    for date_item in date_columns[:6]:
        same_table_measures = [
            measure
            for measure in measure_columns
            if measure.get("table") == date_item.get("table")
        ]
        if not same_table_measures:
            same_table_measures = measure_columns[:3]
        for measure in same_table_measures[:3]:
            trend_candidates.append(
                {
                    "table": date_item["table"],
                    "date_column": date_item["column"],
                    "measure_column": measure["column"],
                    "measure_dtype": measure.get("dtype", ""),
                    "date_range": (
                        f"{date_item.get('min') or '?'} → {date_item.get('max') or '?'}"
                    ),
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
        chart_recommendations.append(
            {
                "title": f"{candidate['measure_column']} over {candidate['date_column']}",
                "table": candidate["table"],
                "chart_type": "line",
                "reason": f"date coverage {candidate['date_range']}",
            }
        )

    notes: list[str] = []
    if not date_columns:
        notes.append("No obvious date columns detected for time-series analysis.")
    if not measure_columns:
        notes.append("No obvious numeric measures detected for charting.")
    if date_columns and measure_columns and not breakout_dimensions:
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
) -> list[dict[str, str]]:
    """Build reusable prompt recipes from deterministic schema overviews."""
    dataset_overview = await build_dataset_overview(schema_info, run_sql)
    dimension_overview = await build_dimension_overview(schema_info, run_sql)
    trend_overview = await build_trend_overview(schema_info, run_sql)

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
                    f"and `{top_trend['measure_column']}`. Create a chart and explain the main pattern in the range {top_trend['date_range']}."
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
