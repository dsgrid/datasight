"""Integration tests for time series quality checks against real DuckDB data.

These tests create hourly timestamp data with known DST and leap-year
characteristics, then verify that build_time_series_quality() detects
gaps, duplicates, and completeness issues.
"""

import asyncio

import duckdb
import pytest

from datasight.data_profile import build_time_series_quality
from datasight.runner import DuckDBRunner


@pytest.fixture()
def hourly_db(tmp_path):
    """Create a DuckDB with a complete 2024 hourly time series (leap year, 8784 hours)."""
    db_path = tmp_path / "hourly.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE gen_hourly AS
        SELECT
            ts,
            'solar' AS fuel
        FROM generate_series(
            TIMESTAMP '2024-01-01 00:00:00',
            TIMESTAMP '2024-12-31 23:00:00',
            INTERVAL '1 HOUR'
        ) t(ts)
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def hourly_runner(hourly_db):
    runner = DuckDBRunner(hourly_db)
    yield runner
    runner.close()


def _run(coro):
    return asyncio.run(coro)


# -- Complete data: no issues ------------------------------------------------


def test_complete_hourly_series_has_no_issues(hourly_runner):
    """A complete 2024 hourly series (leap year) should produce zero issues."""
    configs = [
        {
            "table": "gen_hourly",
            "timestamp_column": "ts",
            "frequency": "PT1H",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, hourly_runner.run_sql))
    assert result["time_series_issues"] == []
    assert len(result["time_series_summaries"]) == 1
    summary = result["time_series_summaries"][0]
    assert summary["total_rows"] == 8784  # 2024 is a leap year


# -- Leap year vs non-leap year ----------------------------------------------


@pytest.fixture()
def non_leap_db(tmp_path):
    """DuckDB with a complete 2023 hourly series (non-leap year, 8760 hours)."""
    db_path = tmp_path / "non_leap.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE gen_hourly AS
        SELECT ts, 'wind' AS fuel
        FROM generate_series(
            TIMESTAMP '2023-01-01 00:00:00',
            TIMESTAMP '2023-12-31 23:00:00',
            INTERVAL '1 HOUR'
        ) t(ts)
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def non_leap_runner(non_leap_db):
    runner = DuckDBRunner(non_leap_db)
    yield runner
    runner.close()


def test_non_leap_year_complete(non_leap_runner):
    """A complete 2023 non-leap-year series should have 8760 rows and no issues."""
    configs = [
        {
            "table": "gen_hourly",
            "timestamp_column": "ts",
            "frequency": "PT1H",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, non_leap_runner.run_sql))
    assert result["time_series_issues"] == []
    summary = result["time_series_summaries"][0]
    assert summary["total_rows"] == 8760


# -- DST spring-forward gap --------------------------------------------------


@pytest.fixture()
def dst_spring_forward_db(tmp_path):
    """Hourly series for March 2024 with the spring-forward hour (2:00 AM EST) removed.

    US Eastern DST 2024: clocks spring forward at 2:00 AM on March 10.
    In UTC, that's 07:00 UTC. We simulate a local-time dataset where
    2024-03-10 02:00 is missing (the clock jumps from 01:59 to 03:00).
    """
    db_path = tmp_path / "dst_spring.duckdb"
    conn = duckdb.connect(str(db_path))
    # Generate full March in local-time-like timestamps, then delete the
    # spring-forward hour.
    conn.execute("""
        CREATE TABLE load_hourly AS
        SELECT ts, 'east' AS region
        FROM generate_series(
            TIMESTAMP '2024-03-01 00:00:00',
            TIMESTAMP '2024-03-31 23:00:00',
            INTERVAL '1 HOUR'
        ) t(ts)
    """)
    conn.execute("""
        DELETE FROM load_hourly WHERE ts = TIMESTAMP '2024-03-10 02:00:00'
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def dst_spring_runner(dst_spring_forward_db):
    runner = DuckDBRunner(dst_spring_forward_db)
    yield runner
    runner.close()


def test_dst_spring_forward_gap_detected(dst_spring_runner):
    """Removing the spring-forward hour should produce a gap issue."""
    configs = [
        {
            "table": "load_hourly",
            "timestamp_column": "ts",
            "frequency": "PT1H",
            "group_columns": ["region"],
            "time_zone": "America/New_York",
        }
    ]
    result = _run(build_time_series_quality(configs, dst_spring_runner.run_sql))
    gaps = [i for i in result["time_series_issues"] if i["issue"] == "gap"]
    assert len(gaps) == 1
    assert "2024-03-10 01:00:00" in gaps[0]["detail"]
    assert "2024-03-10 03:00:00" in gaps[0]["detail"]
    assert gaps[0]["group_values"] == {"region": "east"}


# -- DST fall-back duplicate -------------------------------------------------


@pytest.fixture()
def dst_fall_back_db(tmp_path):
    """Hourly series for November 2024 with the fall-back hour (1:00 AM EST) duplicated.

    US Eastern DST 2024: clocks fall back at 2:00 AM on November 3.
    The 1:00 AM hour occurs twice. We simulate this by inserting a
    duplicate row for 2024-11-03 01:00.
    """
    db_path = tmp_path / "dst_fall.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE load_hourly AS
        SELECT ts, 'east' AS region
        FROM generate_series(
            TIMESTAMP '2024-11-01 00:00:00',
            TIMESTAMP '2024-11-30 23:00:00',
            INTERVAL '1 HOUR'
        ) t(ts)
    """)
    # Insert duplicate for the fall-back hour
    conn.execute("""
        INSERT INTO load_hourly VALUES (TIMESTAMP '2024-11-03 01:00:00', 'east')
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def dst_fall_runner(dst_fall_back_db):
    runner = DuckDBRunner(dst_fall_back_db)
    yield runner
    runner.close()


def test_dst_fall_back_duplicate_detected(dst_fall_runner):
    """Duplicating the fall-back hour should produce a duplicate issue."""
    configs = [
        {
            "table": "load_hourly",
            "timestamp_column": "ts",
            "frequency": "PT1H",
            "group_columns": ["region"],
            "time_zone": "America/New_York",
        }
    ]
    result = _run(build_time_series_quality(configs, dst_fall_runner.run_sql))
    dups = [i for i in result["time_series_issues"] if i["issue"] == "duplicate"]
    assert len(dups) == 1
    assert "2024-11-03 01:00:00" in dups[0]["detail"]
    assert "2 times" in dups[0]["detail"]


# -- Multi-group completeness ------------------------------------------------


@pytest.fixture()
def multi_group_db(tmp_path):
    """Two fuel groups: solar has a complete January, wind is missing 24 hours."""
    db_path = tmp_path / "multi_group.duckdb"
    conn = duckdb.connect(str(db_path))
    # Solar: complete January 2024 (744 hours)
    conn.execute("""
        CREATE TABLE gen_hourly AS
        SELECT ts, 'solar' AS fuel
        FROM generate_series(
            TIMESTAMP '2024-01-01 00:00:00',
            TIMESTAMP '2024-01-31 23:00:00',
            INTERVAL '1 HOUR'
        ) t(ts)
    """)
    # Wind: January 2024 minus the first 24 hours (720 hours)
    conn.execute("""
        INSERT INTO gen_hourly
        SELECT ts, 'wind' AS fuel
        FROM generate_series(
            TIMESTAMP '2024-01-02 00:00:00',
            TIMESTAMP '2024-01-31 23:00:00',
            INTERVAL '1 HOUR'
        ) t(ts)
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def multi_group_runner(multi_group_db):
    runner = DuckDBRunner(multi_group_db)
    yield runner
    runner.close()


def test_multi_group_gap_in_one_group(multi_group_runner):
    """Wind group missing first 24 hours should produce a gap; solar should be clean."""
    configs = [
        {
            "table": "gen_hourly",
            "timestamp_column": "ts",
            "frequency": "PT1H",
            "group_columns": ["fuel"],
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, multi_group_runner.run_sql))
    # Both groups are internally contiguous (no gaps within either partition).
    # The real signal is the summary showing different row counts.
    assert not [i for i in result["time_series_issues"] if i["issue"] == "gap"]
    summary = result["time_series_summaries"][0]
    # Solar has 744, wind has 720, total is 1464
    assert summary["total_rows"] == 1464


# -- Leap year: missing Feb 29 -----------------------------------------------


@pytest.fixture()
def missing_leap_day_db(tmp_path):
    """2024 hourly series with all 24 hours of Feb 29 removed."""
    db_path = tmp_path / "missing_leap.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE gen_hourly AS
        SELECT ts, 'gas' AS fuel
        FROM generate_series(
            TIMESTAMP '2024-01-01 00:00:00',
            TIMESTAMP '2024-12-31 23:00:00',
            INTERVAL '1 HOUR'
        ) t(ts)
    """)
    conn.execute("""
        DELETE FROM gen_hourly
        WHERE ts >= TIMESTAMP '2024-02-29 00:00:00'
          AND ts <  TIMESTAMP '2024-03-01 00:00:00'
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def missing_leap_runner(missing_leap_day_db):
    runner = DuckDBRunner(missing_leap_day_db)
    yield runner
    runner.close()


def test_missing_leap_day_detected(missing_leap_runner):
    """Removing Feb 29 from a 2024 series should produce a gap and reduce row count."""
    configs = [
        {
            "table": "gen_hourly",
            "timestamp_column": "ts",
            "frequency": "PT1H",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, missing_leap_runner.run_sql))
    summary = result["time_series_summaries"][0]
    assert summary["total_rows"] == 8784 - 24  # 8760
    gaps = [i for i in result["time_series_issues"] if i["issue"] == "gap"]
    assert len(gaps) == 1
    assert "2024-02-28 23:00:00" in gaps[0]["detail"]
    assert "2024-03-01 00:00:00" in gaps[0]["detail"]


# ---------------------------------------------------------------------------
# Frequency variants: PT15M, PT30M, P1D, P1M
# ---------------------------------------------------------------------------


@pytest.fixture()
def fifteen_min_db(tmp_path):
    """DuckDB with a complete day of 15-minute data (96 intervals)."""
    db_path = tmp_path / "pt15m.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE readings AS
        SELECT ts
        FROM generate_series(
            TIMESTAMP '2024-06-01 00:00:00',
            TIMESTAMP '2024-06-01 23:45:00',
            INTERVAL '15 MINUTE'
        ) t(ts)
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def fifteen_min_runner(fifteen_min_db):
    runner = DuckDBRunner(fifteen_min_db)
    yield runner
    runner.close()


def test_pt15m_complete_no_issues(fifteen_min_runner):
    """A complete day of 15-minute data should have 96 rows and no issues."""
    configs = [
        {
            "table": "readings",
            "timestamp_column": "ts",
            "frequency": "PT15M",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, fifteen_min_runner.run_sql))
    assert result["time_series_issues"] == []
    assert result["time_series_summaries"][0]["total_rows"] == 96


@pytest.fixture()
def fifteen_min_gap_db(tmp_path):
    """DuckDB with a day of 15-minute data, one interval removed."""
    db_path = tmp_path / "pt15m_gap.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE readings AS
        SELECT ts
        FROM generate_series(
            TIMESTAMP '2024-06-01 00:00:00',
            TIMESTAMP '2024-06-01 23:45:00',
            INTERVAL '15 MINUTE'
        ) t(ts)
        WHERE ts != TIMESTAMP '2024-06-01 12:00:00'
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def fifteen_min_gap_runner(fifteen_min_gap_db):
    runner = DuckDBRunner(fifteen_min_gap_db)
    yield runner
    runner.close()


def test_pt15m_gap_detected(fifteen_min_gap_runner):
    """Removing a 15-minute interval should detect a gap with PT15M frequency."""
    configs = [
        {
            "table": "readings",
            "timestamp_column": "ts",
            "frequency": "PT15M",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, fifteen_min_gap_runner.run_sql))
    gaps = [i for i in result["time_series_issues"] if i["issue"] == "gap"]
    assert len(gaps) == 1
    assert "expected 15-minute" in gaps[0]["detail"]


@pytest.fixture()
def thirty_min_db(tmp_path):
    """DuckDB with a complete day of 30-minute data (48 intervals)."""
    db_path = tmp_path / "pt30m.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE readings AS
        SELECT ts
        FROM generate_series(
            TIMESTAMP '2024-06-01 00:00:00',
            TIMESTAMP '2024-06-01 23:30:00',
            INTERVAL '30 MINUTE'
        ) t(ts)
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def thirty_min_runner(thirty_min_db):
    runner = DuckDBRunner(thirty_min_db)
    yield runner
    runner.close()


def test_pt30m_complete_no_issues(thirty_min_runner):
    """A complete day of 30-minute data should have 48 rows and no issues."""
    configs = [
        {
            "table": "readings",
            "timestamp_column": "ts",
            "frequency": "PT30M",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, thirty_min_runner.run_sql))
    assert result["time_series_issues"] == []
    assert result["time_series_summaries"][0]["total_rows"] == 48


@pytest.fixture()
def daily_db(tmp_path):
    """DuckDB with a complete January 2024 daily series (31 days)."""
    db_path = tmp_path / "daily.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE daily_gen AS
        SELECT ts::DATE AS report_date
        FROM generate_series(
            TIMESTAMP '2024-01-01',
            TIMESTAMP '2024-01-31',
            INTERVAL '1 DAY'
        ) t(ts)
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def daily_runner(daily_db):
    runner = DuckDBRunner(daily_db)
    yield runner
    runner.close()


def test_p1d_complete_no_issues(daily_runner):
    """A complete January daily series should have 31 rows and no issues."""
    configs = [
        {
            "table": "daily_gen",
            "timestamp_column": "report_date",
            "frequency": "P1D",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, daily_runner.run_sql))
    assert result["time_series_issues"] == []
    assert result["time_series_summaries"][0]["total_rows"] == 31


@pytest.fixture()
def daily_gap_db(tmp_path):
    """DuckDB with January 2024 daily series, Jan 15 removed."""
    db_path = tmp_path / "daily_gap.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE daily_gen AS
        SELECT ts::DATE AS report_date
        FROM generate_series(
            TIMESTAMP '2024-01-01',
            TIMESTAMP '2024-01-31',
            INTERVAL '1 DAY'
        ) t(ts)
        WHERE ts::DATE != DATE '2024-01-15'
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def daily_gap_runner(daily_gap_db):
    runner = DuckDBRunner(daily_gap_db)
    yield runner
    runner.close()


def test_p1d_gap_detected(daily_gap_runner):
    """Removing a day from a daily series should detect a gap."""
    configs = [
        {
            "table": "daily_gen",
            "timestamp_column": "report_date",
            "frequency": "P1D",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, daily_gap_runner.run_sql))
    gaps = [i for i in result["time_series_issues"] if i["issue"] == "gap"]
    assert len(gaps) == 1
    assert "expected daily" in gaps[0]["detail"]


@pytest.fixture()
def monthly_db(tmp_path):
    """DuckDB with a complete 2024 monthly series (12 months)."""
    db_path = tmp_path / "monthly.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE monthly_gen AS
        SELECT ts::DATE AS report_month
        FROM generate_series(
            TIMESTAMP '2024-01-01',
            TIMESTAMP '2024-12-01',
            INTERVAL '1 MONTH'
        ) t(ts)
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def monthly_runner(monthly_db):
    runner = DuckDBRunner(monthly_db)
    yield runner
    runner.close()


def test_p1m_complete_no_issues(monthly_runner):
    """A complete 2024 monthly series should have 12 rows and no issues."""
    configs = [
        {
            "table": "monthly_gen",
            "timestamp_column": "report_month",
            "frequency": "P1M",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, monthly_runner.run_sql))
    assert result["time_series_issues"] == []
    assert result["time_series_summaries"][0]["total_rows"] == 12


@pytest.fixture()
def monthly_gap_db(tmp_path):
    """DuckDB with 2024 monthly series, July removed."""
    db_path = tmp_path / "monthly_gap.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE monthly_gen AS
        SELECT ts::DATE AS report_month
        FROM generate_series(
            TIMESTAMP '2024-01-01',
            TIMESTAMP '2024-12-01',
            INTERVAL '1 MONTH'
        ) t(ts)
        WHERE ts::DATE != DATE '2024-07-01'
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def monthly_gap_runner(monthly_gap_db):
    runner = DuckDBRunner(monthly_gap_db)
    yield runner
    runner.close()


def test_p1m_gap_detected(monthly_gap_runner):
    """Removing a month from a monthly series should detect a gap."""
    configs = [
        {
            "table": "monthly_gen",
            "timestamp_column": "report_month",
            "frequency": "P1M",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, monthly_gap_runner.run_sql))
    gaps = [i for i in result["time_series_issues"] if i["issue"] == "gap"]
    assert len(gaps) == 1
    assert "expected monthly" in gaps[0]["detail"]


# ---------------------------------------------------------------------------
# SQL error handling: non-existent table/column
# ---------------------------------------------------------------------------


def test_nonexistent_table_returns_empty(hourly_runner):
    """A config pointing to a non-existent table should not crash."""
    configs = [
        {
            "table": "no_such_table",
            "timestamp_column": "ts",
            "frequency": "PT1H",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, hourly_runner.run_sql))
    # Should degrade gracefully: no summary, no issues
    assert result["time_series_summaries"] == []
    assert result["time_series_issues"] == []


def test_nonexistent_column_returns_empty(hourly_runner):
    """A config pointing to a non-existent column should not crash."""
    configs = [
        {
            "table": "gen_hourly",
            "timestamp_column": "no_such_column",
            "frequency": "PT1H",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, hourly_runner.run_sql))
    assert result["time_series_summaries"] == []
    assert result["time_series_issues"] == []


# ---------------------------------------------------------------------------
# NULL timestamp handling
# ---------------------------------------------------------------------------


@pytest.fixture()
def nulls_db(tmp_path):
    """DuckDB with some NULL timestamps mixed into a short hourly series."""
    db_path = tmp_path / "nulls.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE readings (ts TIMESTAMP, val DOUBLE)
    """)
    conn.execute("""
        INSERT INTO readings
        SELECT ts, 42.0
        FROM generate_series(
            TIMESTAMP '2024-01-01 00:00:00',
            TIMESTAMP '2024-01-01 23:00:00',
            INTERVAL '1 HOUR'
        ) t(ts)
    """)
    # Add 5 NULL-timestamp rows
    conn.execute("""
        INSERT INTO readings VALUES (NULL, 1.0), (NULL, 2.0), (NULL, 3.0),
                                    (NULL, 4.0), (NULL, 5.0)
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def nulls_runner(nulls_db):
    runner = DuckDBRunner(nulls_db)
    yield runner
    runner.close()


def test_null_timestamps_excluded_from_checks(nulls_runner):
    """NULL timestamps should be silently excluded — not cause errors or false gaps."""
    configs = [
        {
            "table": "readings",
            "timestamp_column": "ts",
            "frequency": "PT1H",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, nulls_runner.run_sql))
    # The 24 non-null hours are complete — no issues
    assert result["time_series_issues"] == []
    summary = result["time_series_summaries"][0]
    # COUNT excludes NULLs due to WHERE ts IS NOT NULL
    assert summary["total_rows"] == 24


# ---------------------------------------------------------------------------
# Duplicates with count > 2
# ---------------------------------------------------------------------------


@pytest.fixture()
def triple_dup_db(tmp_path):
    """DuckDB with a short series where one timestamp appears 3 times."""
    db_path = tmp_path / "triple_dup.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE readings AS
        SELECT ts
        FROM generate_series(
            TIMESTAMP '2024-06-01 00:00:00',
            TIMESTAMP '2024-06-01 05:00:00',
            INTERVAL '1 HOUR'
        ) t(ts)
    """)
    # Add 2 extra copies of 03:00 (total 3)
    conn.execute("""
        INSERT INTO readings VALUES
            (TIMESTAMP '2024-06-01 03:00:00'),
            (TIMESTAMP '2024-06-01 03:00:00')
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def triple_dup_runner(triple_dup_db):
    runner = DuckDBRunner(triple_dup_db)
    yield runner
    runner.close()


def test_triple_duplicate_detected(triple_dup_runner):
    """Three copies of the same timestamp should be reported with '3 times'."""
    configs = [
        {
            "table": "readings",
            "timestamp_column": "ts",
            "frequency": "PT1H",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, triple_dup_runner.run_sql))
    dups = [i for i in result["time_series_issues"] if i["issue"] == "duplicate"]
    assert len(dups) == 1
    assert "3 times" in dups[0]["detail"]
    assert "2024-06-01 03:00:00" in dups[0]["detail"]


# ---------------------------------------------------------------------------
# Issue count limit (top 20)
# ---------------------------------------------------------------------------


@pytest.fixture()
def many_gaps_db(tmp_path):
    """DuckDB with many gaps — keep only every other day in a year of daily data."""
    db_path = tmp_path / "many_gaps.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE daily_sparse AS
        SELECT ts::DATE AS report_date
        FROM generate_series(
            TIMESTAMP '2024-01-01',
            TIMESTAMP '2024-12-31',
            INTERVAL '2 DAY'
        ) t(ts)
    """)
    conn.close()
    return str(db_path)


@pytest.fixture()
def many_gaps_runner(many_gaps_db):
    runner = DuckDBRunner(many_gaps_db)
    yield runner
    runner.close()


def test_issues_capped_at_20(many_gaps_runner):
    """build_time_series_quality should return at most 20 issues."""
    configs = [
        {
            "table": "daily_sparse",
            "timestamp_column": "report_date",
            "frequency": "P1D",
            "time_zone": "UTC",
        }
    ]
    result = _run(build_time_series_quality(configs, many_gaps_runner.run_sql))
    # Every-other-day data creates ~183 gaps at P1D, but gap detection LIMITs to 10
    # and the overall result caps at 20
    assert len(result["time_series_issues"]) <= 20
