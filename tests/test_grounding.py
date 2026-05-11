"""Tests for the grounding-drift detector."""

from __future__ import annotations

import textwrap
from pathlib import Path

import duckdb

from datasight.grounding import (
    DriftItem,
    DriftReport,
    build_enum_values_sync,
    build_schema_truth_sync,
    check_grounding_drift,
    format_drift_report,
)


def _make_db(tmp_path: Path, rows: list[tuple]) -> str:
    """Build a tiny long-format DuckDB and return its path."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE load_data "
        "(geography VARCHAR, fuel_type VARCHAR, end_use VARCHAR, "
        "time_year BIGINT, energy_mwh DOUBLE)"
    )
    for row in rows:
        conn.execute("INSERT INTO load_data VALUES (?, ?, ?, ?, ?)", row)
    conn.close()
    return str(db_path)


def test_build_schema_truth_sync_returns_table_to_columns():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE t1 (a INT, b VARCHAR)")
    conn.execute("CREATE TABLE t2 (c DOUBLE)")
    truth = build_schema_truth_sync(conn)
    assert truth == {"t1": {"a", "b"}, "t2": {"c"}}


def test_build_enum_values_sync_collects_distinct_strings(tmp_path):
    db_path = _make_db(
        tmp_path,
        [
            ("pacific", "elec", "heating", 2020, 1.0),
            ("pacific", "ng", "cooling", 2020, 2.0),
            ("south_atlantic", "elec", "heating", 2020, 3.0),
        ],
    )
    conn = duckdb.connect(db_path, read_only=True)
    truth = build_schema_truth_sync(conn)
    values = build_enum_values_sync(conn, truth)
    assert "pacific" in values
    assert "south_atlantic" in values
    assert "elec" in values
    assert "ng" in values
    assert "heating" in values
    assert "cooling" in values


def test_build_enum_values_sync_skips_high_cardinality(tmp_path):
    db_path = tmp_path / "big.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE t (label VARCHAR)")
    for i in range(50):
        conn.execute("INSERT INTO t VALUES (?)", (f"label_{i}",))
    conn.close()
    conn = duckdb.connect(str(db_path), read_only=True)
    truth = build_schema_truth_sync(conn)
    values = build_enum_values_sync(conn, truth, max_per_column=20)
    assert values == set()


def test_check_clean_grounding_reports_no_drift(tmp_path):
    truth = {"load_data": {"geography", "fuel_type", "energy_mwh"}}
    (tmp_path / "queries.yaml").write_text(
        textwrap.dedent("""
        - question: "Total energy by region"
          sql: |
            SELECT geography, SUM(energy_mwh) AS total
            FROM load_data
            WHERE fuel_type = 'elec'
            GROUP BY geography;
    """).strip()
    )
    report = check_grounding_drift(tmp_path, truth, enum_values={"elec"})
    assert report.is_clean


def test_queries_yaml_missing_column_is_flagged(tmp_path):
    truth = {"load_data": {"geography", "fuel_type", "energy_mwh"}}
    (tmp_path / "queries.yaml").write_text(
        textwrap.dedent("""
        - question: "Stale"
          sql: SELECT elec_heating FROM load_data;
    """).strip()
    )
    report = check_grounding_drift(tmp_path, truth)
    assert not report.is_clean
    assert any(item.claim == "elec_heating" for item in report.items)


def test_queries_yaml_missing_table_is_flagged(tmp_path):
    truth = {"load_data": {"geography"}}
    (tmp_path / "queries.yaml").write_text(
        textwrap.dedent("""
        - question: "Wrong table"
          sql: SELECT geography FROM missing_table;
    """).strip()
    )
    report = check_grounding_drift(tmp_path, truth)
    assert any(item.kind == "table" and item.claim == "missing_table" for item in report.items)


def test_queries_yaml_cte_name_is_not_flagged_as_missing_table(tmp_path):
    truth = {"load_data": {"geography", "energy_mwh"}}
    (tmp_path / "queries.yaml").write_text(
        textwrap.dedent("""
        - question: "CTE chain"
          sql: |
            WITH yearly AS (
              SELECT geography, SUM(energy_mwh) AS total FROM load_data GROUP BY geography
            )
            SELECT * FROM yearly;
    """).strip()
    )
    report = check_grounding_drift(tmp_path, truth)
    assert report.is_clean, [item.detail for item in report.items]


def test_queries_yaml_output_alias_is_not_flagged(tmp_path):
    truth = {"load_data": {"geography", "energy_mwh"}}
    (tmp_path / "queries.yaml").write_text(
        textwrap.dedent("""
        - question: "Aliased output"
          sql: |
            SELECT geography, SUM(energy_mwh) AS total_energy
            FROM load_data
            GROUP BY geography
            ORDER BY total_energy DESC;
    """).strip()
    )
    report = check_grounding_drift(tmp_path, truth)
    assert report.is_clean, [item.detail for item in report.items]


def test_schema_description_md_flags_missing_column_reference(tmp_path):
    truth = {"load_data": {"geography", "fuel_type"}}
    (tmp_path / "schema_description.md").write_text(
        "# Schema\n\nThe `elec_heating` column tracks electricity heating.\n"
    )
    report = check_grounding_drift(tmp_path, truth)
    assert any(item.claim == "elec_heating" and item.line == 3 for item in report.items)


def test_schema_description_md_qualified_table_column_resolves(tmp_path):
    truth = {"load_data": {"geography", "fuel_type"}}
    (tmp_path / "schema_description.md").write_text(
        "# Schema\n\nUse `load_data.geography` for region filtering.\n"
    )
    report = check_grounding_drift(tmp_path, truth)
    assert report.is_clean


def test_schema_description_md_qualified_unknown_column_is_flagged(tmp_path):
    truth = {"load_data": {"geography"}}
    (tmp_path / "schema_description.md").write_text(
        "# Schema\n\nUse `load_data.elec_heating` for heating.\n"
    )
    report = check_grounding_drift(tmp_path, truth)
    assert any(item.claim == "load_data.elec_heating" for item in report.items)


def test_schema_description_md_enum_values_are_allowlisted(tmp_path):
    truth = {"load_data": {"geography"}}
    (tmp_path / "schema_description.md").write_text(
        "# Schema\n\nValues: `pacific`, `mountain`, `new_england`.\n"
    )
    report = check_grounding_drift(
        tmp_path, truth, enum_values={"pacific", "mountain", "new_england"}
    )
    assert report.is_clean


def test_time_series_yaml_missing_table_is_flagged(tmp_path):
    truth = {"load_data": {"geography"}}
    (tmp_path / "time_series.yaml").write_text(
        textwrap.dedent("""
        - table: missing_table
          timestamp_column: ts
          frequency: PT1H
    """).strip()
    )
    report = check_grounding_drift(tmp_path, truth)
    assert any(item.kind == "ts_table" and item.claim == "missing_table" for item in report.items)


def test_time_series_yaml_missing_timestamp_column_is_flagged(tmp_path):
    truth = {"load_data": {"geography"}}
    (tmp_path / "time_series.yaml").write_text(
        textwrap.dedent("""
        - table: load_data
          timestamp_column: ts
          frequency: PT1H
    """).strip()
    )
    report = check_grounding_drift(tmp_path, truth)
    assert any(item.kind == "ts_column" and item.claim == "ts" for item in report.items)


def test_time_series_yaml_missing_group_column_is_flagged(tmp_path):
    truth = {"load_data": {"geography", "ts"}}
    (tmp_path / "time_series.yaml").write_text(
        textwrap.dedent("""
        - table: load_data
          timestamp_column: ts
          group_columns: [geography, missing_dim]
          frequency: PT1H
    """).strip()
    )
    report = check_grounding_drift(tmp_path, truth)
    assert any(item.kind == "ts_column" and item.claim == "missing_dim" for item in report.items)


def test_missing_grounding_files_are_silently_skipped(tmp_path):
    truth = {"load_data": {"geography"}}
    report = check_grounding_drift(tmp_path, truth)
    assert report.is_clean


def test_format_drift_report_shows_per_file_breakdown():
    report = DriftReport(
        items=[
            DriftItem(
                file="a/queries.yaml",
                line=None,
                kind="column",
                claim="foo",
                detail="foo not found",
                suggestion="bar",
            ),
            DriftItem(
                file="a/schema_description.md",
                line=10,
                kind="column",
                claim="baz",
                detail="baz not found",
            ),
        ]
    )
    text = format_drift_report(report)
    assert "queries.yaml" in text
    assert "schema_description.md" in text
    assert "foo" in text
    assert "did you mean: bar" in text
    assert ":10" in text


def test_format_drift_report_clean_returns_clean_message():
    text = format_drift_report(DriftReport())
    assert "no drift" in text.lower()


def test_drift_report_by_file_groups_items():
    report = DriftReport(
        items=[
            DriftItem(file="x", line=None, kind="column", claim="a", detail=""),
            DriftItem(file="y", line=None, kind="column", claim="b", detail=""),
            DriftItem(file="x", line=None, kind="column", claim="c", detail=""),
        ]
    )
    grouped = report.by_file()
    assert list(grouped.keys()) == ["x", "y"]
    assert len(grouped["x"]) == 2
    assert len(grouped["y"]) == 1


def test_queries_yaml_with_invalid_yaml_reports_parse_error(tmp_path):
    truth = {"load_data": {"x"}}
    (tmp_path / "queries.yaml").write_text("- this: is\n  not: valid: yaml:")
    report = check_grounding_drift(tmp_path, truth)
    assert any(item.kind == "parse_error" for item in report.items)
