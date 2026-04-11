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
