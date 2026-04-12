"""Tests for datasight.distribution."""

from __future__ import annotations

import duckdb
import pytest

from datasight.distribution import (
    _check_energy_flags,
    _compute_percentiles,
    _compute_stats,
    _count_iqr_outliers,
    _detect_spikes,
    build_distribution_overview,
)


@pytest.fixture
def energy_conn(tmp_path):
    """Create a DuckDB with energy-ish data for distribution tests."""
    db = tmp_path / "energy.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute("""
        CREATE TABLE generation (
            plant_id INTEGER,
            report_date DATE,
            net_generation_mwh DOUBLE,
            capacity_factor DOUBLE,
            heat_rate_mmbtu_per_mwh DOUBLE,
            price_usd_per_mwh DOUBLE,
            fuel_consumed_mmbtu DOUBLE
        )
    """)
    # Build a distribution with some zero/negative values and one big outlier
    rows = []
    for i in range(100):
        rows.append(
            (
                1,
                f"2024-{(i % 12) + 1:02d}-01",
                float(i),  # net_generation_mwh
                0.5,  # capacity_factor (ratio)
                10.0,  # heat_rate
                50.0,  # price
                100.0,
            )
        )
    # Throw in a negative generation (data error)
    rows.append((1, "2024-06-15", -5.0, 1.5, 2.5, 0.0, 100.0))
    # An outlier spike
    rows.append((1, "2024-07-01", 10000.0, 0.6, 12.0, 55.0, 100.0))
    conn.executemany(
        "INSERT INTO generation VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    yield conn
    conn.close()


def _make_run_sql(conn):
    async def run_sql(sql):
        return conn.execute(sql).fetchdf()

    return run_sql


@pytest.mark.asyncio
async def test_compute_percentiles_duckdb(energy_conn):
    run_sql = _make_run_sql(energy_conn)
    out = await _compute_percentiles(run_sql, "generation", "net_generation_mwh")
    assert set(out.keys()) == {"p1", "p5", "q1", "p50", "q3", "p95", "p99"}
    assert out["p50"] is not None


@pytest.mark.asyncio
async def test_compute_stats(energy_conn):
    run_sql = _make_run_sql(energy_conn)
    stats = await _compute_stats(run_sql, "generation", "net_generation_mwh")
    assert stats["total"] == 102
    assert stats["negative_count"] == 1
    assert stats["zero_count"] == 1  # i=0
    assert stats["mean"] is not None


@pytest.mark.asyncio
async def test_compute_stats_failure_returns_empty(energy_conn):
    run_sql = _make_run_sql(energy_conn)
    stats = await _compute_stats(run_sql, "nonexistent_table", "col")
    assert stats == {}


@pytest.mark.asyncio
async def test_count_iqr_outliers(energy_conn):
    run_sql = _make_run_sql(energy_conn)
    n = await _count_iqr_outliers(run_sql, "generation", "net_generation_mwh", 25.0, 75.0)
    # At least the 10000 outlier
    assert n >= 1


@pytest.mark.asyncio
async def test_detect_spikes(energy_conn):
    run_sql = _make_run_sql(energy_conn)
    spikes = await _detect_spikes(run_sql, "generation", "report_date", "net_generation_mwh")
    assert isinstance(spikes, list)


@pytest.mark.asyncio
async def test_detect_spikes_missing_table(energy_conn):
    run_sql = _make_run_sql(energy_conn)
    out = await _detect_spikes(run_sql, "nope", "d", "m")
    assert out == []


def test_check_energy_flags_negative_generation():
    flags = _check_energy_flags(
        "generation",
        "net_generation_mwh",
        {"role": "energy", "unit": "mwh"},
        {"p1": None, "p99": None},
        {"total": 100, "negative_count": 3, "zero_count": 0},
    )
    assert any(f["flag"] == "negative_generation" for f in flags)


def test_check_energy_flags_ratio_over_100():
    flags = _check_energy_flags(
        "t",
        "cap_factor_pct",
        {"role": "ratio", "unit": "pct"},
        {"p1": 0.1, "p99": 150.0},
        {"total": 10, "negative_count": 0, "zero_count": 0},
    )
    assert any(f["flag"] == "capacity_factor_over_100pct" for f in flags)


def test_check_energy_flags_ratio_over_1():
    flags = _check_energy_flags(
        "t",
        "cap_factor",
        {"role": "ratio", "unit": None},
        {"p1": 0.0, "p99": 1.5},
        {"total": 10, "negative_count": 0, "zero_count": 0},
    )
    assert any(f["flag"] == "capacity_factor_over_1" for f in flags)


def test_check_energy_flags_implausible_heat_rate():
    flags = _check_energy_flags(
        "t",
        "heat_rate",
        {"role": "rate", "unit": "mmbtu"},
        {"p1": 0.1, "p99": 100.0},
        {"total": 10, "negative_count": 0, "zero_count": 0},
    )
    assert any(f["flag"] == "implausible_heat_rate" for f in flags)


def test_check_energy_flags_zero_in_rate():
    flags = _check_energy_flags(
        "t",
        "price",
        {"role": "price", "unit": "usd_per_mwh"},
        {"p1": 0.0, "p99": 100.0},
        {"total": 10, "negative_count": 0, "zero_count": 2},
    )
    assert any(f["flag"] == "zero_values_in_rate" for f in flags)


def test_check_energy_flags_no_semantics():
    assert _check_energy_flags("t", "c", None, {}, {}) == []


@pytest.mark.asyncio
async def test_build_distribution_overview(energy_conn):
    """End-to-end distribution build on a realistic schema."""
    run_sql = _make_run_sql(energy_conn)
    schema_info = [
        {
            "name": "generation",
            "row_count": 102,
            "columns": [
                {"name": "plant_id", "dtype": "INTEGER"},
                {"name": "report_date", "dtype": "DATE"},
                {"name": "net_generation_mwh", "dtype": "DOUBLE"},
                {"name": "capacity_factor", "dtype": "DOUBLE"},
                {"name": "heat_rate_mmbtu_per_mwh", "dtype": "DOUBLE"},
                {"name": "price_usd_per_mwh", "dtype": "DOUBLE"},
                {"name": "fuel_consumed_mmbtu", "dtype": "DOUBLE"},
            ],
        }
    ]
    result = await build_distribution_overview(schema_info, run_sql)
    assert result["table_count"] == 1
    assert len(result["distributions"]) > 0
    # A column with a negative energy value should surface at least one energy flag
    assert any(f.get("flag") == "negative_generation" for f in result["energy_flags"])


@pytest.mark.asyncio
async def test_build_distribution_overview_empty_schema():
    async def run_sql(sql):
        raise AssertionError("should not be called")

    result = await build_distribution_overview([], run_sql)
    assert result["distributions"] == []
    assert "No numeric" in " ".join(result["notes"])


@pytest.mark.asyncio
async def test_build_distribution_overview_target_column(energy_conn):
    run_sql = _make_run_sql(energy_conn)
    schema_info = [
        {
            "name": "generation",
            "row_count": 102,
            "columns": [
                {"name": "net_generation_mwh", "dtype": "DOUBLE"},
                {"name": "capacity_factor", "dtype": "DOUBLE"},
            ],
        }
    ]
    result = await build_distribution_overview(
        schema_info, run_sql, target_column="generation.capacity_factor"
    )
    cols = [d["column"] for d in result["distributions"]]
    assert cols == ["capacity_factor"]
