"""Extra tests for data_profile covering edge branches."""

from __future__ import annotations

import duckdb
import pytest

from datasight.data_profile import (
    _apply_measure_override,
    _build_override_measure,
    _get_date_coverage,
    _get_dimension_stats,
    _get_numeric_stats,
    _get_sample_values,
    _infer_measure_semantics,
    _looks_like_count,
    _recommended_chart_type,
    _run_scalar,
    build_column_profile,
    build_dataset_overview,
    build_dimension_overview,
    build_measure_overview,
    build_prompt_recipes,
    build_quality_overview,
    build_table_profile,
    build_time_series_quality,
    build_trend_overview,
    find_column_info,
    find_table_info,
    format_measure_overrides_yaml,
    format_measure_prompt_context,
    format_time_series_prompt_context,
    format_time_series_yaml,
)


@pytest.fixture
def energy_conn(tmp_path):
    db = tmp_path / "e.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute("""
        CREATE TABLE generation (
            plant_id INTEGER,
            report_date DATE,
            ts TIMESTAMP,
            net_generation_mwh DOUBLE,
            capacity_mw DOUBLE,
            capacity_factor DOUBLE,
            price_usd_per_mwh DOUBLE,
            fuel_type VARCHAR,
            state VARCHAR
        )
    """)
    rows = []
    for i in range(24):
        rows.append(
            (
                1,
                "2024-01-01",
                f"2024-01-01 {i:02d}:00:00",
                float(i * 10),
                500.0,
                0.5,
                40.0,
                "coal" if i % 2 == 0 else "gas",
                "CA",
            )
        )
    # Add nulls
    rows.append((2, None, None, None, None, None, None, None, None))
    conn.executemany(
        "INSERT INTO generation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    yield conn
    conn.close()


def _rs(conn):
    async def run(sql):
        return conn.execute(sql).fetchdf()

    return run


# ---------------------------------------------------------------------------
# Pure-function coverage
# ---------------------------------------------------------------------------


def test_looks_like_count_positive():
    assert _looks_like_count("customer_count") is True
    assert _looks_like_count("meters") is True


def test_looks_like_count_negative():
    assert _looks_like_count("net_generation_mwh") is False


def test_infer_measure_semantics_price():
    out = _infer_measure_semantics("price_usd_per_mwh", "DOUBLE", [])
    assert out is not None
    assert out["role"] == "price"


def test_infer_measure_semantics_rate_mmbtu_with_weight():
    out = _infer_measure_semantics("heat_rate_mmbtu", "DOUBLE", ["net_generation_mwh"])
    assert out is not None
    assert out["role"] == "rate"
    assert out["weight_column"] == "net_generation_mwh"
    assert out["average_strategy"] == "weighted_avg"


def test_infer_measure_semantics_ratio():
    out = _infer_measure_semantics("capacity_factor", "DOUBLE", [])
    assert out is not None
    assert out["role"] == "ratio"


def test_infer_measure_semantics_capacity():
    out = _infer_measure_semantics("capacity_mw", "DOUBLE", [])
    assert out is not None
    assert out["role"] in ("capacity", "power")  # depends on token order


def test_infer_measure_semantics_power_peak():
    out = _infer_measure_semantics("peak_demand_mw", "DOUBLE", [])
    assert out is not None
    assert out["role"] == "power"
    assert out["default_aggregation"] == "max"


def test_infer_measure_semantics_count_like():
    out = _infer_measure_semantics("customer_count", "INTEGER", [])
    assert out is not None
    assert out["role"] == "count"


def test_infer_measure_semantics_total_like():
    out = _infer_measure_semantics("gross_amount", "DOUBLE", [])
    assert out is not None
    assert out["default_aggregation"] == "sum"


def test_infer_measure_semantics_identifier_returns_none():
    assert _infer_measure_semantics("customer_id", "INTEGER", []) is None


def test_infer_measure_semantics_non_numeric_returns_none():
    assert _infer_measure_semantics("name", "VARCHAR", []) is None


def test_recommended_chart_type_area():
    item = {"role": "energy", "aggregation": "sum"}
    assert _recommended_chart_type(item) == "area"


def test_recommended_chart_type_line_for_percent():
    assert _recommended_chart_type({"format": "percent"}) == "line"


def test_recommended_chart_type_preferred_wins():
    assert _recommended_chart_type({"preferred_chart_types": ["scatter"]}) == "scatter"


def test_apply_measure_override_none_passthrough():
    inferred = {"column": "x", "default_aggregation": "avg"}
    assert _apply_measure_override(inferred, None) == inferred


def test_apply_measure_override_merges_expression():
    inferred = {"column": "x", "default_aggregation": "avg"}
    out = _apply_measure_override(inferred, {"expression": "a+b", "role": "energy"})
    assert out["expression"] == "a+b"
    assert out["source"] == "calculated"
    assert out["role"] == "energy"


def test_build_override_measure():
    out = _build_override_measure(
        "t", {"column": "c", "expression": "a+b", "name": "calc"}, ["a", "b"]
    )
    assert out["table"] == "t"
    assert out["expression"] == "a+b"
    assert out["source"] == "calculated"


def test_find_table_info_and_column_info():
    schema = [{"name": "Foo", "columns": [{"name": "Bar"}]}]
    t = find_table_info(schema, "foo")
    assert t is not None
    col = find_column_info(t, "bar")
    assert col is not None
    assert col["name"] == "Bar"
    assert find_table_info(schema, "nope") is None
    assert find_column_info(t, "nope") is None


def test_format_time_series_prompt_context_empty():
    assert format_time_series_prompt_context([]) == ""


def test_format_time_series_prompt_context_populated():
    out = format_time_series_prompt_context(
        [{"table": "t", "timestamp_column": "ts", "frequency": "PT1H", "group_columns": ["r"]}]
    )
    assert "t.ts" in out
    assert "PT1H" in out


def test_format_time_series_yaml_no_candidates():
    out = format_time_series_yaml([])
    assert "No timestamp columns detected" in out


def test_format_time_series_yaml_with_candidates():
    schema = [
        {
            "name": "events",
            "row_count": 500,
            "columns": [
                {"name": "id", "dtype": "INTEGER"},
                {"name": "ts", "dtype": "TIMESTAMP"},
            ],
        },
        # Skipped: too few rows
        {
            "name": "small",
            "row_count": 10,
            "columns": [{"name": "ts", "dtype": "TIMESTAMP"}],
        },
    ]
    out = format_time_series_yaml(schema)
    assert "events" in out
    assert "ts" in out


# ---------------------------------------------------------------------------
# async helpers edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_scalar_query_exception():
    async def run(sql):
        raise RuntimeError("boom")

    result = await _run_scalar(run, "SELECT 1", "value")
    assert result is None


@pytest.mark.asyncio
async def test_run_scalar_missing_column():
    import pandas as pd

    async def run(sql):
        return pd.DataFrame({"other": [1]})

    assert await _run_scalar(run, "", "value") is None


@pytest.mark.asyncio
async def test_get_date_coverage_query_error(energy_conn):
    out = await _get_date_coverage(_rs(energy_conn), "nonexistent", "x")
    assert out is None


@pytest.mark.asyncio
async def test_get_date_coverage_all_null(energy_conn):
    """All-null yields a dict with NaT strings (pandas converts MIN/MAX of nulls to NaT)."""
    energy_conn.execute("CREATE TABLE d_null AS SELECT NULL::DATE AS d")
    out = await _get_date_coverage(_rs(energy_conn), "d_null", "d")
    # Depending on pandas version this is either None or a dict with NaT strings
    assert out is None or "NaT" in str(out.get("min"))


@pytest.mark.asyncio
async def test_get_dimension_stats_query_error(energy_conn):
    out = await _get_dimension_stats(_rs(energy_conn), "nonexistent", "x", 1)
    assert out is None


@pytest.mark.asyncio
async def test_get_dimension_stats_empty_table_returns_none_counts(energy_conn):
    """An empty table makes SUM(...) return SQL NULL → pandas NaN; counts must
    fall back to None instead of raising ValueError when cast to int."""
    energy_conn.execute("CREATE TABLE empty_dim (label VARCHAR)")
    out = await _get_dimension_stats(_rs(energy_conn), "empty_dim", "label", 0)
    assert out is not None
    assert out["distinct_count"] == 0
    assert out["null_count"] is None
    assert out["null_rate"] is None
    assert out["sample_values"] == []


@pytest.mark.asyncio
async def test_get_numeric_stats_query_error(energy_conn):
    out = await _get_numeric_stats(_rs(energy_conn), "nonexistent", "x")
    assert out is None


@pytest.mark.asyncio
async def test_get_sample_values_query_error(energy_conn):
    out = await _get_sample_values(_rs(energy_conn), "nonexistent", "x")
    assert out == []


# ---------------------------------------------------------------------------
# Full builders
# ---------------------------------------------------------------------------


def _schema_info():
    return [
        {
            "name": "generation",
            "row_count": 25,
            "columns": [
                {"name": "plant_id", "dtype": "INTEGER"},
                {"name": "report_date", "dtype": "DATE"},
                {"name": "ts", "dtype": "TIMESTAMP"},
                {"name": "net_generation_mwh", "dtype": "DOUBLE"},
                {"name": "capacity_mw", "dtype": "DOUBLE"},
                {"name": "capacity_factor", "dtype": "DOUBLE"},
                {"name": "price_usd_per_mwh", "dtype": "DOUBLE"},
                {"name": "fuel_type", "dtype": "VARCHAR"},
                {"name": "state", "dtype": "VARCHAR"},
            ],
        }
    ]


@pytest.mark.asyncio
async def test_build_quality_overview_smoke(energy_conn):
    out = await build_quality_overview(_schema_info(), _rs(energy_conn))
    assert "null_columns" in out
    assert "numeric_flags" in out
    assert "date_columns" in out
    # capacity_mw is constant → should be flagged
    flags = [f["issue"] for f in out["numeric_flags"]]
    assert any("constant" in f for f in flags)


@pytest.mark.asyncio
async def test_build_measure_overview_with_overrides(energy_conn):
    overrides = [
        {
            "table": "generation",
            "column": "capacity_factor",
            "role": "ratio",
            "reason": "custom",
        },
        # A calculated measure (expression + name)
        {
            "table": "generation",
            "name": "gen_per_capacity",
            "expression": "net_generation_mwh / capacity_mw",
            "role": "ratio",
            "reason": "calculated",
        },
    ]
    out = await build_measure_overview(_schema_info(), _rs(energy_conn), overrides=overrides)
    assert any(m.get("expression") for m in out["measures"])


@pytest.mark.asyncio
async def test_format_measure_overrides_yaml(energy_conn):
    overview = await build_measure_overview(_schema_info(), _rs(energy_conn))
    yaml_text = format_measure_overrides_yaml(overview)
    assert "table" in yaml_text


@pytest.mark.asyncio
async def test_format_measure_prompt_context(energy_conn):
    overview = await build_measure_overview(_schema_info(), _rs(energy_conn))
    text = format_measure_prompt_context(overview)
    assert "Inferred Measure Semantics" in text


def test_format_measure_prompt_context_empty():
    assert format_measure_prompt_context({"measures": []}) == ""


@pytest.mark.asyncio
async def test_build_trend_overview(energy_conn):
    out = await build_trend_overview(_schema_info(), _rs(energy_conn))
    assert "trend_candidates" in out
    assert "chart_recommendations" in out


@pytest.mark.asyncio
async def test_build_prompt_recipes(energy_conn):
    recipes = await build_prompt_recipes(_schema_info(), _rs(energy_conn))
    assert len(recipes) > 0
    titles = [r["title"] for r in recipes]
    assert any("Profile" in t for t in titles)


@pytest.mark.asyncio
async def test_build_table_profile(energy_conn):
    schema = _schema_info()[0]
    out = await build_table_profile(schema, _rs(energy_conn))
    assert out["table"] == "generation"
    assert out["column_count"] == 9
    # report_date column has one null
    assert any(c["column"] == "report_date" for c in out["null_columns"])


@pytest.mark.asyncio
async def test_build_column_profile_numeric(energy_conn):
    schema = _schema_info()[0]
    col = {"name": "net_generation_mwh", "dtype": "DOUBLE"}
    out = await build_column_profile(schema, col, _rs(energy_conn))
    assert out["column"] == "net_generation_mwh"
    assert out.get("numeric_stats") is not None


@pytest.mark.asyncio
async def test_build_column_profile_date(energy_conn):
    schema = _schema_info()[0]
    col = {"name": "report_date", "dtype": "DATE"}
    out = await build_column_profile(schema, col, _rs(energy_conn))
    assert "date_coverage" in out


@pytest.mark.asyncio
async def test_build_column_profile_text(energy_conn):
    schema = _schema_info()[0]
    col = {"name": "fuel_type", "dtype": "VARCHAR"}
    out = await build_column_profile(schema, col, _rs(energy_conn))
    assert "dimension_stats" in out


@pytest.mark.asyncio
async def test_build_table_profile_empty_table(energy_conn):
    """Empty table → SUM returns NaN; profile must not raise on int(NaN)."""
    energy_conn.execute("CREATE TABLE empty_tbl (label VARCHAR, value INTEGER)")
    schema = {
        "name": "empty_tbl",
        "row_count": 0,
        "columns": [
            {"name": "label", "dtype": "VARCHAR"},
            {"name": "value", "dtype": "INTEGER"},
        ],
    }
    out = await build_table_profile(schema, _rs(energy_conn))
    assert out["table"] == "empty_tbl"
    assert out["null_columns"] == []


@pytest.mark.asyncio
async def test_build_column_profile_empty_table(energy_conn):
    """Empty table → SUM returns NaN; column profile must not raise."""
    energy_conn.execute("CREATE TABLE empty_col (label VARCHAR)")
    schema = {
        "name": "empty_col",
        "row_count": 0,
        "columns": [{"name": "label", "dtype": "VARCHAR"}],
    }
    col = {"name": "label", "dtype": "VARCHAR"}
    out = await build_column_profile(schema, col, _rs(energy_conn))
    assert out["null_count"] is None
    assert out["distinct_count"] == 0
    assert out["null_rate"] is None


@pytest.mark.asyncio
async def test_build_column_profile_other_type(energy_conn):
    """Fall-through branch: neither numeric/date/text -> sample_values path."""
    energy_conn.execute("CREATE TABLE bools (b BOOLEAN)")
    energy_conn.execute("INSERT INTO bools VALUES (TRUE), (FALSE)")
    schema = {"name": "bools", "row_count": 2, "columns": [{"name": "b", "dtype": "BOOLEAN"}]}
    col = {"name": "b", "dtype": "BOOLEAN"}
    out = await build_column_profile(schema, col, _rs(energy_conn))
    assert "sample_values" in out


@pytest.mark.asyncio
async def test_build_time_series_quality(energy_conn):
    configs = [
        {
            "table": "generation",
            "timestamp_column": "ts",
            "frequency": "PT1H",
            "group_columns": [],
            "time_zone": "UTC",
        }
    ]
    out = await build_time_series_quality(configs, _rs(energy_conn))
    assert "time_series_summaries" in out
    assert "time_series_issues" in out


@pytest.mark.asyncio
async def test_build_time_series_quality_unknown_frequency(energy_conn):
    configs = [{"table": "generation", "timestamp_column": "ts", "frequency": "UNKNOWN"}]
    out = await build_time_series_quality(configs, _rs(energy_conn))
    assert out["time_series_summaries"] == []


@pytest.mark.asyncio
async def test_build_dataset_overview(energy_conn):
    out = await build_dataset_overview(_schema_info(), _rs(energy_conn))
    assert out["table_count"] == 1
    assert out["total_rows"] == 25


@pytest.mark.asyncio
async def test_build_dimension_overview(energy_conn):
    out = await build_dimension_overview(_schema_info(), _rs(energy_conn))
    assert "dimension_columns" in out
    assert "join_hints" in out
