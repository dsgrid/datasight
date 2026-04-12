"""Extra tests for datasight.audit_report renderers."""

from __future__ import annotations

from dataclasses import asdict

import pytest

from datasight.audit_report import (
    _fmt,
    _md_table,
    build_audit_report,
    render_audit_report_html,
    render_audit_report_markdown,
)
from datasight.runner import DuckDBRunner
from datasight.schema import introspect_schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def test_md_table_empty_rows_returns_empty_string():
    assert _md_table(["a", "b"], []) == ""


def test_md_table_renders_headers_and_rows():
    out = _md_table(["A", "B"], [["1", "2"], ["3", "4"]])
    assert "| A | B |" in out
    assert "| 1 | 2 |" in out
    assert "| 3 | 4 |" in out


def test_fmt_handles_none_float_and_other():
    assert _fmt(None) == "-"
    assert _fmt(None, default="N/A") == "N/A"
    assert _fmt(1.23456789) == "1.235"
    assert _fmt(42) == "42"
    assert _fmt("hello") == "hello"


# ---------------------------------------------------------------------------
# Markdown renderer
# ---------------------------------------------------------------------------


def _rich_audit_data():
    return {
        "generated_at": "2026-04-12T00:00:00+00:00",
        "project_name": "Test Project",
        "dataset_overview": {
            "table_count": 2,
            "total_rows": 100,
            "total_columns": 9,
        },
        "quality": {
            "null_columns": [
                {"table": "orders", "column": "note", "null_count": 5, "null_rate": 0.05}
            ],
            "numeric_flags": [
                {"table": "orders", "column": "qty", "issue": "negative values present"}
            ],
            "notes": ["sample quality note"],
        },
        "integrity": {
            "primary_keys": [
                {"table": "orders", "column": "id", "is_unique": True},
                {"table": "products", "column": "id", "is_unique": False},
            ],
            "duplicate_keys": [{"table": "products", "column": "id", "duplicate_count": 2}],
            "orphan_foreign_keys": [
                {
                    "child_table": "orders",
                    "child_column": "product_id",
                    "parent_table": "products",
                    "parent_column": "id",
                    "orphan_count": 1,
                    "child_rows": 10,
                }
            ],
            "join_explosions": [
                {
                    "table_a": "orders",
                    "table_b": "products",
                    "join_column": "product_id",
                    "expected_rows": 10,
                    "actual_rows": 20,
                    "explosion_factor": 2.0,
                }
            ],
            "notes": ["integrity note"],
        },
        "distribution": {
            "distributions": [
                {
                    "table": "orders",
                    "column": "qty",
                    "p5": 1.0,
                    "p50": 5.5,
                    "p95": 99.0,
                    "zero_rate": 0.1,
                    "negative_rate": None,
                    "outlier_count": 3,
                }
            ],
            "energy_flags": [{"table": "generation", "column": "mwh", "detail": "sudden spike"}],
            "spikes": [{"detail": "peak on 2026-01-02"}],
            "notes": ["distribution note"],
        },
        "validation": {
            "rule_count": 3,
            "summary": {"pass": 1, "fail": 1, "warn": 1},
            "results": [
                {
                    "table": "orders",
                    "rule": "required_columns",
                    "column": None,
                    "status": "pass",
                    "detail": "All columns present.",
                },
                {
                    "table": "orders",
                    "rule": "max_null_rate",
                    "column": "note",
                    "status": "fail",
                    "detail": "Null rate 50% exceeds threshold.",
                },
                {
                    "table": "orders",
                    "rule": "numeric_range",
                    "column": "qty",
                    "status": "warn",
                    "detail": "No data.",
                },
            ],
        },
    }


def test_render_audit_report_markdown_full():
    md = render_audit_report_markdown(_rich_audit_data())
    assert "datasight Audit Report — Test Project" in md
    assert "## Dataset Overview" in md
    assert "Null-heavy Columns" in md
    assert "Numeric Range Flags" in md
    assert "Primary Keys" in md
    assert "Duplicate Keys" in md
    assert "Orphan Foreign Keys" in md
    assert "Join Explosion Risks" in md
    assert "Distributions" in md
    assert "Energy Flags" in md
    assert "Temporal Spikes" in md
    assert "Validation Rules" in md
    # PK "is_unique: False" should render as NO
    assert "NO" in md
    # Statuses uppercased
    assert "PASS" in md and "FAIL" in md and "WARN" in md


def test_render_audit_report_markdown_minimal():
    md = render_audit_report_markdown(
        {
            "generated_at": "2026-04-12T00:00:00+00:00",
            "project_name": None,
            "dataset_overview": None,
            "quality": None,
            "integrity": None,
            "distribution": None,
            "validation": None,
        }
    )
    # Default title with no project name
    assert md.splitlines()[0] == "# datasight Audit Report"
    assert "## Data Quality" in md
    assert "## Referential Integrity" in md


# ---------------------------------------------------------------------------
# HTML renderer
# ---------------------------------------------------------------------------


def test_render_audit_report_html_with_validation():
    html = render_audit_report_html(_rich_audit_data())
    assert "Test Project" in html
    assert "orders" in html


def test_render_audit_report_html_without_validation():
    data = _rich_audit_data()
    data["validation"] = None
    data["project_name"] = None
    html = render_audit_report_html(data)
    assert html  # Non-empty string


# ---------------------------------------------------------------------------
# build_audit_report end-to-end
# ---------------------------------------------------------------------------


def _schema_info(tables):
    return [
        {
            "name": t.name,
            "row_count": t.row_count,
            "columns": [asdict(c) for c in t.columns],
        }
        for t in tables
    ]


@pytest.mark.asyncio
async def test_build_audit_report_end_to_end(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    try:
        tables = await introspect_schema(runner.run_sql, runner=runner)
        schema_info = _schema_info(tables)
        report = await build_audit_report(
            schema_info,
            runner.run_sql,
            project_name="Test",
        )
    finally:
        runner.close()

    assert report["project_name"] == "Test"
    assert "dataset_overview" in report
    assert "quality" in report
    assert "integrity" in report
    assert "distribution" in report
    assert report["validation"] is None


@pytest.mark.asyncio
async def test_build_audit_report_with_validation_rules(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    rules = [
        {
            "table": "orders",
            "rules": [
                {"type": "required_columns", "columns": ["id", "product_id"]},
            ],
        }
    ]
    try:
        tables = await introspect_schema(runner.run_sql, runner=runner)
        schema_info = _schema_info(tables)
        report = await build_audit_report(
            schema_info,
            runner.run_sql,
            validation_rules=rules,
        )
    finally:
        runner.close()

    assert report["validation"] is not None
    # Render both formats to exercise the full renderer paths over a real report
    md = render_audit_report_markdown(report)
    assert "Validation Rules" in md
    html = render_audit_report_html(report)
    assert html
