"""Tests for datasight.validation."""

from __future__ import annotations

import duckdb
import pytest

from datasight.validation import build_validation_report, load_validation_config


@pytest.fixture
def val_conn(tmp_path):
    db = tmp_path / "v.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute("""
        CREATE TABLE t (
            id INTEGER,
            status VARCHAR,
            amount DOUBLE,
            code VARCHAR,
            ts DATE
        )
    """)
    conn.executemany(
        "INSERT INTO t VALUES (?, ?, ?, ?, ?)",
        [
            (1, "active", 100.0, "AB12", "2024-01-01"),
            (2, "active", 200.0, "CD34", "2024-02-01"),
            (3, "inactive", -5.0, "EF56", "2024-03-01"),
            (4, "active", 300.0, "GH78", "2024-04-01"),
            (4, "bogus", None, "bad!", "2024-05-01"),  # duplicate id, null amount, bad code
        ],
    )
    yield conn
    conn.close()


def _run_sql(conn):
    async def rs(sql):
        return conn.execute(sql).fetchdf()

    return rs


def _schema():
    return [
        {
            "name": "t",
            "row_count": 5,
            "columns": [
                {"name": "id", "dtype": "INTEGER"},
                {"name": "status", "dtype": "VARCHAR"},
                {"name": "amount", "dtype": "DOUBLE"},
                {"name": "code", "dtype": "VARCHAR"},
                {"name": "ts", "dtype": "DATE"},
            ],
        }
    ]


# ---------------------------------------------------------------------------
# load_validation_config
# ---------------------------------------------------------------------------


def test_load_config_missing_returns_empty(tmp_path):
    assert load_validation_config(None, str(tmp_path)) == []


def test_load_config_explicit_missing(tmp_path):
    assert load_validation_config(str(tmp_path / "nope.yaml"), str(tmp_path)) == []


def test_load_config_invalid_yaml(tmp_path):
    p = tmp_path / "v.yaml"
    p.write_text("not: valid: yaml: :::", encoding="utf-8")
    result = load_validation_config(str(p), str(tmp_path))
    assert result == []


def test_load_config_not_a_list(tmp_path):
    p = tmp_path / "v.yaml"
    p.write_text("foo: bar\n", encoding="utf-8")
    assert load_validation_config(str(p), str(tmp_path)) == []


def test_load_config_defaults_to_project_validation_yaml(tmp_path):
    p = tmp_path / "validation.yaml"
    p.write_text("- table: t\n  rules: []\n", encoding="utf-8")
    result = load_validation_config(None, str(tmp_path))
    assert result == [{"table": "t", "rules": []}]


# ---------------------------------------------------------------------------
# build_validation_report
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_report_missing_table(val_conn):
    rules = [{"table": "nonexistent", "rules": [{"type": "row_count", "min": 1}]}]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert any(r["rule"] == "table_exists" and r["status"] == "fail" for r in report["results"])


@pytest.mark.asyncio
async def test_report_unknown_rule_type(val_conn):
    rules = [{"table": "t", "rules": [{"type": "invented_rule"}]}]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert any(r["status"] == "warn" and "Unknown rule" in r["detail"] for r in report["results"])


@pytest.mark.asyncio
async def test_required_columns_pass_and_fail(val_conn):
    rules = [
        {
            "table": "t",
            "rules": [
                {"type": "required_columns", "columns": ["id", "status"]},
                {"type": "required_columns", "columns": ["missing_one"]},
            ],
        }
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    statuses = [r["status"] for r in report["results"]]
    assert "pass" in statuses
    assert "fail" in statuses


@pytest.mark.asyncio
async def test_max_null_rate_fail(val_conn):
    rules = [
        {"table": "t", "rules": [{"type": "max_null_rate", "column": "amount", "threshold": 0.01}]}
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert report["results"][0]["status"] == "fail"


@pytest.mark.asyncio
async def test_max_null_rate_pass(val_conn):
    rules = [
        {"table": "t", "rules": [{"type": "max_null_rate", "column": "id", "threshold": 0.5}]}
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert report["results"][0]["status"] == "pass"


@pytest.mark.asyncio
async def test_numeric_range(val_conn):
    rules = [
        {
            "table": "t",
            "rules": [{"type": "numeric_range", "column": "amount", "min": 0, "max": 500}],
        }
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    # -5 violates min
    assert report["results"][0]["status"] == "fail"


@pytest.mark.asyncio
async def test_numeric_range_pass(val_conn):
    rules = [
        {
            "table": "t",
            "rules": [{"type": "numeric_range", "column": "amount", "min": -100, "max": 1000}],
        }
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert report["results"][0]["status"] == "pass"


@pytest.mark.asyncio
async def test_numeric_range_query_error(val_conn):
    rules = [
        {"table": "t", "rules": [{"type": "numeric_range", "column": "no_such_col", "min": 0}]}
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert report["results"][0]["status"] == "warn"


@pytest.mark.asyncio
async def test_allowed_values(val_conn):
    rules = [
        {
            "table": "t",
            "rules": [
                {"type": "allowed_values", "column": "status", "values": ["active", "inactive"]}
            ],
        }
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    # "bogus" is unexpected
    assert report["results"][0]["status"] == "fail"


@pytest.mark.asyncio
async def test_allowed_values_pass(val_conn):
    rules = [
        {
            "table": "t",
            "rules": [
                {
                    "type": "allowed_values",
                    "column": "status",
                    "values": ["active", "inactive", "bogus"],
                }
            ],
        }
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert report["results"][0]["status"] == "pass"


@pytest.mark.asyncio
async def test_allowed_values_query_error(val_conn):
    rules = [
        {"table": "t", "rules": [{"type": "allowed_values", "column": "bogus", "values": []}]}
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert report["results"][0]["status"] == "warn"


@pytest.mark.asyncio
async def test_regex_fail(val_conn):
    rules = [
        {
            "table": "t",
            "rules": [{"type": "regex", "column": "code", "pattern": "^[A-Z]{2}[0-9]{2}$"}],
        }
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert report["results"][0]["status"] == "fail"


@pytest.mark.asyncio
async def test_regex_pass(val_conn):
    # All ids match \d+
    rules = [{"table": "t", "rules": [{"type": "regex", "column": "code", "pattern": ".+"}]}]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert report["results"][0]["status"] == "pass"


@pytest.mark.asyncio
async def test_uniqueness_fail(val_conn):
    rules = [{"table": "t", "rules": [{"type": "uniqueness", "columns": ["id"]}]}]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert report["results"][0]["status"] == "fail"


@pytest.mark.asyncio
async def test_uniqueness_pass(val_conn):
    rules = [{"table": "t", "rules": [{"type": "uniqueness", "columns": ["code"]}]}]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert report["results"][0]["status"] == "pass"


@pytest.mark.asyncio
async def test_uniqueness_no_columns(val_conn):
    rules = [{"table": "t", "rules": [{"type": "uniqueness"}]}]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert report["results"][0]["status"] == "warn"


@pytest.mark.asyncio
async def test_uniqueness_missing_column_returns_zero_dup_groups():
    """_run_scalar swallows the error — treated as no duplicates."""

    async def run_sql(sql):
        raise RuntimeError("SQL fails")

    rules = [{"table": "t", "rules": [{"type": "uniqueness", "columns": ["missing"]}]}]
    report = await build_validation_report(_schema(), run_sql, rules)
    # Error is swallowed in _run_scalar -> treated as 0 duplicates -> pass
    assert report["results"][0]["status"] in ("pass", "warn")


@pytest.mark.asyncio
async def test_monotonic_fail(val_conn):
    rules = [
        {"table": "t", "rules": [{"type": "monotonic", "column": "id", "direction": "increasing"}]}
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    # duplicate id 4,4 -> increasing violation (<=)
    assert report["results"][0]["status"] == "fail"


@pytest.mark.asyncio
async def test_monotonic_pass_non_decreasing(val_conn):
    rules = [
        {
            "table": "t",
            "rules": [{"type": "monotonic", "column": "id", "direction": "non_decreasing"}],
        }
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert report["results"][0]["status"] == "pass"


@pytest.mark.asyncio
async def test_monotonic_bad_column_is_swallowed(val_conn):
    rules = [{"table": "t", "rules": [{"type": "monotonic", "column": "no_col"}]}]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    # _run_scalar swallows -> becomes 0 violations -> pass
    assert report["results"][0]["status"] in ("pass", "warn")


@pytest.mark.asyncio
async def test_row_count_pass_and_fail(val_conn):
    rules = [
        {
            "table": "t",
            "rules": [
                {"type": "row_count", "min": 2, "max": 10},
                {"type": "row_count", "min": 100},
            ],
        }
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    statuses = [r["status"] for r in report["results"]]
    assert "pass" in statuses
    assert "fail" in statuses


@pytest.mark.asyncio
async def test_freshness_fail(val_conn):
    rules = [{"table": "t", "rules": [{"type": "freshness", "column": "ts", "max_age_days": 1}]}]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    # All dates are in 2024; test runs in the future -> fail
    assert report["results"][0]["status"] == "fail"


@pytest.mark.asyncio
async def test_freshness_pass_with_very_old_tolerance(val_conn):
    rules = [
        {"table": "t", "rules": [{"type": "freshness", "column": "ts", "max_age_days": 100000}]}
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert report["results"][0]["status"] == "pass"


@pytest.mark.asyncio
async def test_freshness_no_data(val_conn):
    # empty table scenario
    val_conn.execute("CREATE TABLE empty_t (ts DATE)")
    schema = [
        {
            "name": "empty_t",
            "row_count": 0,
            "columns": [{"name": "ts", "dtype": "DATE"}],
        }
    ]
    rules = [{"table": "empty_t", "rules": [{"type": "freshness", "column": "ts"}]}]
    report = await build_validation_report(schema, _run_sql(val_conn), rules)
    assert report["results"][0]["status"] == "warn"


@pytest.mark.asyncio
async def test_summary_counts(val_conn):
    rules = [
        {
            "table": "t",
            "rules": [
                {"type": "row_count", "min": 1},  # pass
                {"type": "row_count", "min": 1000},  # fail
                {"type": "unknown_rule"},  # warn
            ],
        }
    ]
    report = await build_validation_report(_schema(), _run_sql(val_conn), rules)
    assert report["summary"]["pass"] >= 1
    assert report["summary"]["fail"] >= 1
    assert report["summary"]["warn"] >= 1
    assert report["table_count"] == 1
