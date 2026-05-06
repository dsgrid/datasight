"""Tests for untidy-dataset detection in datasight.tidy."""

from __future__ import annotations

import json
import re

import duckdb
import pytest
from click.testing import CliRunner

from datasight.cli import cli
from datasight.tidy import (
    MIN_GROUP_SIZE,
    _classify_period_token,
    _split_prefix_period,
    analyze_tidy_patterns,
)

from tests._env_helpers import DATASIGHT_ENV_VARS, scrub_datasight_env


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Scrub datasight env vars before AND after each test in this module."""
    for key in DATASIGHT_ENV_VARS:
        monkeypatch.delenv(key, raising=False)
    yield
    scrub_datasight_env()


def _table(name: str, columns: list[tuple[str, str]], row_count: int | None = 0) -> dict:
    return {
        "name": name,
        "row_count": row_count,
        "columns": [{"name": n, "dtype": dt, "nullable": True} for n, dt in columns],
    }


# ---------------------------------------------------------------------------
# Period token classifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "token,expected_kind",
    [
        ("2020", "year"),
        ("y2020", "year"),
        ("year2024", "year"),
        ("2020_01", "year_month"),
        ("2020-12", "year_month"),
        ("202012", "year_month"),
        ("2020_q1", "year_quarter"),
        ("2024Q4", "year_quarter"),
        ("q1", "quarter"),
        ("q4", "quarter"),
        ("jan", "month"),
        ("january", "month"),
        ("hour_01", "hour"),
        ("hour23", "hour"),
        ("h00", "hour"),
        ("hr_3", "hour"),
        ("day_01", "day"),
        ("day31", "day"),
        ("m_07", "month_num"),
        ("m12", "month_num"),
        ("month_03", "month_num"),
        ("jan_2020", "month_year"),
        ("2020_jan", "year_month_word"),
    ],
)
def test_classify_period_token(token, expected_kind):
    assert _classify_period_token(token) == expected_kind


@pytest.mark.parametrize(
    "name",
    ["plant_id", "id", "fuel_type", "net_generation_mwh", "report_date", "name"],
)
def test_classify_period_token_rejects_normal_columns(name):
    assert _classify_period_token(name) is None


@pytest.mark.parametrize(
    "name,expected",
    [
        ("sales_2020", ("sales", "2020", "year")),
        ("revenue_2020_q1", ("revenue", "2020_q1", "year_quarter")),
        ("hour_01", ("", "hour_01", "hour")),
        ("h00", ("", "h00", "hour")),
        ("q1", ("", "q1", "quarter")),
        ("2020", ("", "2020", "year")),
        ("plant_id", None),
    ],
)
def test_split_prefix_period(name, expected):
    assert _split_prefix_period(name) == expected


# ---------------------------------------------------------------------------
# analyze_tidy_patterns: detection cases
# ---------------------------------------------------------------------------


def test_detects_year_columns_with_shared_prefix():
    schema = [
        _table(
            "sales_wide",
            [
                ("region", "VARCHAR"),
                ("sales_2020", "DOUBLE"),
                ("sales_2021", "DOUBLE"),
                ("sales_2022", "DOUBLE"),
                ("sales_2023", "DOUBLE"),
            ],
            row_count=50,
        )
    ]
    out = analyze_tidy_patterns(schema)
    assert len(out["suggestions"]) == 1
    s = out["suggestions"][0]
    assert s["pattern"] == "repeated_prefix_period"
    assert s["dimensions"] == [{"name": "year", "kind": "date_period"}]
    assert [m["column"] for m in s["column_mappings"]] == [
        "sales_2020",
        "sales_2021",
        "sales_2022",
        "sales_2023",
    ]
    assert [m["dimension_values"] for m in s["column_mappings"]] == [
        {"year": "2020"},
        {"year": "2021"},
        {"year": "2022"},
        {"year": "2023"},
    ]
    assert s["id_columns"] == ["region"]
    assert s["value_column"] == "sales"
    assert s["target_object_name"] == "sales_wide_long"
    assert s["confidence"] == "high"
    assert s["source"] == "deterministic"
    assert "UNPIVOT" in s["reshape_sql"]
    assert "CREATE OR REPLACE TABLE" in s["reshape_sql"]


def test_detects_hour_columns_no_prefix():
    schema = [
        _table(
            "load_profile",
            [("plant_id", "INTEGER"), ("date", "DATE")]
            + [(f"hour_{i:02d}", "DOUBLE") for i in range(24)],
            row_count=365,
        )
    ]
    out = analyze_tidy_patterns(schema)
    assert len(out["suggestions"]) == 1
    s = out["suggestions"][0]
    assert s["pattern"] == "date_in_column_names"
    assert s["dimensions"] == [{"name": "hour", "kind": "date_period"}]
    assert len(s["column_mappings"]) == 24
    assert s["value_column"] == "value"
    assert s["id_columns"] == ["plant_id", "date"]


def test_detects_quarter_columns():
    schema = [
        _table(
            "kpi",
            [
                ("metric", "VARCHAR"),
                ("q1", "DOUBLE"),
                ("q2", "DOUBLE"),
                ("q3", "DOUBLE"),
                ("q4", "DOUBLE"),
            ],
            row_count=10,
        )
    ]
    out = analyze_tidy_patterns(schema)
    assert len(out["suggestions"]) == 1
    assert out["suggestions"][0]["dimensions"] == [{"name": "quarter", "kind": "date_period"}]


def test_clean_table_produces_no_suggestions():
    schema = [
        _table(
            "generation",
            [
                ("plant_id", "INTEGER"),
                ("report_date", "DATE"),
                ("fuel_type", "VARCHAR"),
                ("net_generation_mwh", "DOUBLE"),
            ],
            row_count=10000,
        )
    ]
    out = analyze_tidy_patterns(schema)
    assert out["suggestions"] == []
    assert out["wide_tables"] == []
    assert out["notes"] == ["No untidy column-shape patterns detected."]


def test_below_min_group_size_is_not_flagged():
    schema = [
        _table(
            "tiny_wide",
            [("region", "VARCHAR"), ("sales_2020", "DOUBLE"), ("sales_2021", "DOUBLE")],
            row_count=5,
        )
    ]
    out = analyze_tidy_patterns(schema)
    assert MIN_GROUP_SIZE == 3
    assert out["suggestions"] == []


def test_detects_wide_table_with_no_period_pattern():
    columns = [("id", "INTEGER")] + [(f"feature_{i}", "DOUBLE") for i in range(35)]
    schema = [_table("feature_dump", columns, row_count=10)]
    out = analyze_tidy_patterns(schema)
    assert out["suggestions"] == []
    assert len(out["wide_tables"]) == 1
    note = out["wide_tables"][0]
    assert note["table"] == "feature_dump"
    assert note["column_count"] == 36


def test_wide_check_skipped_when_period_pattern_already_detected():
    columns = [("id", "INTEGER")] + [(f"y{2000 + i}", "DOUBLE") for i in range(40)]
    schema = [_table("annual_history", columns, row_count=5)]
    out = analyze_tidy_patterns(schema)
    assert len(out["suggestions"]) == 1
    assert out["wide_tables"] == []


def test_wide_check_respects_row_count_threshold():
    columns = [(f"feature_{i}", "DOUBLE") for i in range(35)]
    schema = [_table("ml_features", columns, row_count=10000)]
    out = analyze_tidy_patterns(schema)
    assert out["wide_tables"] == []


def test_multiple_groups_in_same_table():
    schema = [
        _table(
            "mixed",
            [
                ("region", "VARCHAR"),
                ("sales_2020", "DOUBLE"),
                ("sales_2021", "DOUBLE"),
                ("sales_2022", "DOUBLE"),
                ("cost_q1", "DOUBLE"),
                ("cost_q2", "DOUBLE"),
                ("cost_q3", "DOUBLE"),
            ],
        )
    ]
    out = analyze_tidy_patterns(schema)
    suggestions = out["suggestions"]
    assert len(suggestions) == 2
    value_columns = {s["value_column"] for s in suggestions}
    assert value_columns == {"sales", "cost"}


def test_multi_group_id_columns_exclude_sibling_period_columns():
    """When a table has two period groups, neither suggestion should keep the
    other group's pivoted columns as id columns — that would defeat the tidy
    reshape by carrying duplicated wide measures into the long form."""
    schema = [
        _table(
            "mixed",
            [
                ("region", "VARCHAR"),
                ("year_started", "INTEGER"),
                ("sales_2020", "DOUBLE"),
                ("sales_2021", "DOUBLE"),
                ("sales_2022", "DOUBLE"),
                ("cost_q1", "DOUBLE"),
                ("cost_q2", "DOUBLE"),
                ("cost_q3", "DOUBLE"),
            ],
        )
    ]
    out = analyze_tidy_patterns(schema)
    suggestions = out["suggestions"]
    assert len(suggestions) == 2
    for s in suggestions:
        assert s["id_columns"] == ["region", "year_started"]
        for col in ("sales_2020", "sales_2021", "cost_q1", "cost_q2"):
            assert col not in s["id_columns"]


# ---------------------------------------------------------------------------
# Generated reshape SQL must round-trip in DuckDB (and survive view storage)
# ---------------------------------------------------------------------------


def test_reshape_sql_executes_in_duckdb(tmp_path):
    db_path = tmp_path / "wide.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE sales_wide ("
        "region VARCHAR, "
        "sales_2020 DOUBLE, sales_2021 DOUBLE, sales_2022 DOUBLE, sales_2023 DOUBLE)"
    )
    conn.execute(
        "INSERT INTO sales_wide VALUES ('north', 10, 20, 30, 40), ('south', 5, 15, 25, 35)"
    )

    schema = [
        _table(
            "sales_wide",
            [
                ("region", "VARCHAR"),
                ("sales_2020", "DOUBLE"),
                ("sales_2021", "DOUBLE"),
                ("sales_2022", "DOUBLE"),
                ("sales_2023", "DOUBLE"),
            ],
            row_count=2,
        )
    ]
    out = analyze_tidy_patterns(schema)
    sql = out["suggestions"][0]["reshape_sql"]
    conn.execute(sql)
    rows = conn.execute(
        "SELECT region, year, sales FROM sales_wide_long ORDER BY region, year"
    ).fetchall()
    conn.close()
    assert rows == [
        ("north", "2020", 10.0),
        ("north", "2021", 20.0),
        ("north", "2022", 30.0),
        ("north", "2023", 40.0),
        ("south", "2020", 5.0),
        ("south", "2021", 15.0),
        ("south", "2022", 25.0),
        ("south", "2023", 35.0),
    ]


# ---------------------------------------------------------------------------
# Multi-pivot reshape: hand-built TidySuggestion exercising the UNION ALL
# builder for both view and table mode. This is what `tidy review` will
# emit for LLM-proposed multi-axis reshapes (e.g., fuel x year as columns).
# ---------------------------------------------------------------------------


def test_multi_pivot_reshape_round_trips_in_duckdb(tmp_path):
    """Hand-build a 2-dimension `TidySuggestion` and confirm the generated
    UNION ALL DDL produces the expected long form. The deterministic
    detector won't propose this shape; this test pins the multi-pivot
    builder so `tidy review` has a known-good apply path."""
    from datasight.tidy import ColumnMapping, Dimension, TidySuggestion, _build_reshape_sql

    db_path = tmp_path / "multi.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE generation_wide ("
        "plant_id INTEGER, "
        "coal_2020 DOUBLE, coal_2021 DOUBLE, "
        "gas_2020 DOUBLE,  gas_2021 DOUBLE)"
    )
    conn.execute(
        "INSERT INTO generation_wide VALUES (1, 100, 110, 200, 220), (2,  50,  60, 150, 160)"
    )

    dimensions = [
        Dimension(name="fuel_type", kind="category"),
        Dimension(name="year", kind="date_period"),
    ]
    column_mappings = [
        ColumnMapping(column="coal_2020", dimension_values={"fuel_type": "coal", "year": "2020"}),
        ColumnMapping(column="coal_2021", dimension_values={"fuel_type": "coal", "year": "2021"}),
        ColumnMapping(column="gas_2020", dimension_values={"fuel_type": "gas", "year": "2020"}),
        ColumnMapping(column="gas_2021", dimension_values={"fuel_type": "gas", "year": "2021"}),
    ]
    sql = _build_reshape_sql(
        table="generation_wide",
        id_columns=["plant_id"],
        dimensions=dimensions,
        column_mappings=column_mappings,
        value_column="net_generation_mwh",
        target_object_name="generation_long",
        mode="table",
    )
    # Multi-pivot must use UNION ALL even in table mode — UNPIVOT can't emit
    # multiple dimension columns. Pin that here so the dispatch doesn't
    # silently regress. The header comment mentions UNPIVOT in prose, so
    # check the body only (mirrors the single-pivot view assertion above).
    assert "UNION ALL" in sql
    body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    assert "UNPIVOT" not in body
    conn.execute(sql)
    rows = conn.execute(
        "SELECT plant_id, fuel_type, year, net_generation_mwh "
        "FROM generation_long ORDER BY plant_id, fuel_type, year"
    ).fetchall()
    conn.close()
    assert rows == [
        (1, "coal", "2020", 100.0),
        (1, "coal", "2021", 110.0),
        (1, "gas", "2020", 200.0),
        (1, "gas", "2021", 220.0),
        (2, "coal", "2020", 50.0),
        (2, "coal", "2021", 60.0),
        (2, "gas", "2020", 150.0),
        (2, "gas", "2021", 160.0),
    ]

    # Build-then-query the suggestion via `TidySuggestion.build_sql` to make
    # sure the dataclass adapter routes through the same builder.
    sugg = TidySuggestion(
        pattern="repeated_prefix_period",
        table="generation_wide",
        dimensions=dimensions,
        column_mappings=column_mappings,
        id_columns=["plant_id"],
        value_column="net_generation_mwh",
        target_object_name="generation_long_via_dataclass",
        rationale="multi-pivot test fixture",
        reshape_sql=sql,
    )
    assert sugg.affected_columns == ["coal_2020", "coal_2021", "gas_2020", "gas_2021"]
    conn = duckdb.connect(str(db_path))
    conn.execute(sugg.build_sql("view"))
    count = conn.execute("SELECT COUNT(*) FROM generation_long_via_dataclass").fetchone()
    conn.close()
    assert count is not None
    assert count[0] == 8


# ---------------------------------------------------------------------------
# CLI integration: `datasight quality` surfaces tidy suggestions
# ---------------------------------------------------------------------------


@pytest.fixture
def wide_project(tmp_path):
    db_path = tmp_path / "wide.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE sales_wide ("
        "region VARCHAR, "
        "sales_2020 DOUBLE, sales_2021 DOUBLE, sales_2022 DOUBLE, sales_2023 DOUBLE)"
    )
    conn.execute("INSERT INTO sales_wide VALUES ('north', 10, 20, 30, 40)")
    conn.close()
    (tmp_path / ".env").write_text(
        f"LLM_PROVIDER=ollama\nOLLAMA_MODEL=qwen2.5:7b\nDB_MODE=duckdb\nDB_PATH={db_path}\n",
        encoding="utf-8",
    )
    return str(tmp_path)


def test_quality_cli_emits_tidy_suggestions_json(wide_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["quality", "--project-dir", wide_project, "--format", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert "tidy_suggestions" in data
    assert len(data["tidy_suggestions"]) == 1
    s = data["tidy_suggestions"][0]
    assert s["table"] == "sales_wide"
    assert s["dimensions"] == [{"name": "year", "kind": "date_period"}]
    assert "UNPIVOT" in s["reshape_sql"]


def test_quality_cli_emits_tidy_suggestions_markdown(wide_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["quality", "--project-dir", wide_project, "--format", "markdown"])
    assert result.exit_code == 0, result.output
    assert "Tidy Reshape Suggestions" in result.output
    assert "sales_wide" in result.output
    assert "UNPIVOT" in result.output


# ---------------------------------------------------------------------------
# `datasight tidy` command
# ---------------------------------------------------------------------------


def _wide_project_dir(tmp_path) -> tuple[str, str]:
    """Return (project_dir, db_path) for a single-table wide-CSV project."""
    db_path = tmp_path / "wide.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE sales_wide ("
        "region VARCHAR, "
        "sales_2020 DOUBLE, sales_2021 DOUBLE, sales_2022 DOUBLE, sales_2023 DOUBLE)"
    )
    conn.execute(
        "INSERT INTO sales_wide VALUES ('north', 10, 20, 30, 40), ('south', 5, 15, 25, 35)"
    )
    conn.close()
    (tmp_path / ".env").write_text(
        f"LLM_PROVIDER=ollama\nOLLAMA_MODEL=qwen2.5:7b\nDB_MODE=duckdb\nDB_PATH={db_path}\n",
        encoding="utf-8",
    )
    return str(tmp_path), str(db_path)


def test_tidy_suggest_lists_by_default(tmp_path):
    project_dir, _ = _wide_project_dir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["tidy", "suggest", "--project-dir", project_dir])
    assert result.exit_code == 0, result.output
    assert "sales_wide" in result.output
    assert "Suggestions" in result.output


def test_tidy_suggest_json_output(tmp_path):
    project_dir, _ = _wide_project_dir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["tidy", "suggest", "--project-dir", project_dir, "--format", "json"]
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["table_count"] == 1
    assert len(data["suggestions"]) == 1
    assert "applied" not in data


def test_tidy_suggest_markdown_output(tmp_path):
    project_dir, _ = _wide_project_dir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["tidy", "suggest", "--project-dir", project_dir, "--format", "markdown"]
    )
    assert result.exit_code == 0, result.output
    assert "# Tidy Reshape Suggestions" in result.output
    assert "## Suggestions" in result.output


def test_tidy_view_dry_run_does_not_apply(tmp_path):
    project_dir, db_path = _wide_project_dir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["tidy", "view", "--project-dir", project_dir, "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "Would apply" in result.output
    assert "CREATE OR REPLACE VIEW" in result.output
    conn = duckdb.connect(db_path)
    views = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
    conn.close()
    assert "sales_wide_long" not in views


def test_tidy_table_dry_run_emits_table_ddl(tmp_path):
    project_dir, db_path = _wide_project_dir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["tidy", "table", "--project-dir", project_dir, "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "CREATE OR REPLACE TABLE" in result.output
    conn = duckdb.connect(db_path)
    rows = conn.execute("SHOW TABLES").fetchall()
    conn.close()
    assert all("long" not in name for (name,) in rows)


def test_tidy_view_sql_uses_union_all_workaround(tmp_path):
    """View mode must use UNION ALL — UNPIVOT inside a view fails to re-bind
    on a fresh DuckDB connection (1.5.2 binder bug). This test pins the
    workaround so a future "simplification" back to UNPIVOT doesn't silently
    re-introduce the bug."""
    project_dir, db_path = _wide_project_dir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["tidy", "view", "--project-dir", project_dir, "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "UNION ALL" in result.output
    # Strip leading SQL comments before checking — the workaround note
    # mentions UNPIVOT in prose, but the actual SQL must not use it.
    body = "\n".join(
        line for line in result.output.splitlines() if not line.lstrip().startswith("--")
    )
    assert "UNPIVOT" not in body


def test_tidy_table_sql_uses_unpivot(tmp_path):
    """Table mode uses the canonical UNPIVOT form (works because materialized
    tables don't re-bind on query)."""
    project_dir, _ = _wide_project_dir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["tidy", "table", "--project-dir", project_dir, "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "UNPIVOT" in result.output
    assert "UNION ALL" not in result.output


def test_tidy_view_actually_creates_view(tmp_path):
    project_dir, db_path = _wide_project_dir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["tidy", "view", "--project-dir", project_dir])
    assert result.exit_code == 0, result.output
    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT region, year, sales FROM sales_wide_long ORDER BY region, year"
    ).fetchall()
    conn.close()
    assert rows == [
        ("north", "2020", 10.0),
        ("north", "2021", 20.0),
        ("north", "2022", 30.0),
        ("north", "2023", 40.0),
        ("south", "2020", 5.0),
        ("south", "2021", 15.0),
        ("south", "2022", 25.0),
        ("south", "2023", 35.0),
    ]


def test_tidy_table_materializes(tmp_path):
    project_dir, db_path = _wide_project_dir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["tidy", "table", "--project-dir", project_dir])
    assert result.exit_code == 0, result.output
    conn = duckdb.connect(db_path)
    kind = conn.execute(
        "SELECT table_type FROM information_schema.tables WHERE table_name = 'sales_wide_long'"
    ).fetchone()
    row = conn.execute("SELECT COUNT(*) FROM sales_wide_long").fetchone()
    conn.close()
    assert kind is not None
    assert kind[0] == "BASE TABLE"
    assert row is not None
    assert row[0] == 8


def test_tidy_help_lists_subcommands():
    runner = CliRunner()
    result = runner.invoke(cli, ["tidy", "--help"])
    assert result.exit_code == 0
    assert "suggest" in result.output
    assert "view" in result.output
    assert "table" in result.output


def test_tidy_suggest_table_filter_scopes_detection(tmp_path):
    db_path = tmp_path / "multi.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE sales_wide ("
        "region VARCHAR, sales_2020 DOUBLE, sales_2021 DOUBLE, sales_2022 DOUBLE)"
    )
    conn.execute("INSERT INTO sales_wide VALUES ('n', 1, 2, 3)")
    conn.execute(
        "CREATE TABLE costs_wide ("
        "region VARCHAR, cost_q1 DOUBLE, cost_q2 DOUBLE, cost_q3 DOUBLE, cost_q4 DOUBLE)"
    )
    conn.execute("INSERT INTO costs_wide VALUES ('n', 1, 2, 3, 4)")
    conn.close()
    (tmp_path / ".env").write_text(
        f"LLM_PROVIDER=ollama\nOLLAMA_MODEL=qwen2.5:7b\nDB_MODE=duckdb\nDB_PATH={db_path}\n",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "tidy",
            "suggest",
            "--project-dir",
            str(tmp_path),
            "--table",
            "sales_wide",
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["table_count"] == 1
    assert len(data["suggestions"]) == 1
    assert data["suggestions"][0]["table"] == "sales_wide"


def test_tidy_view_table_filter_scopes_apply(tmp_path):
    db_path = tmp_path / "multi.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE sales_wide ("
        "region VARCHAR, sales_2020 DOUBLE, sales_2021 DOUBLE, sales_2022 DOUBLE)"
    )
    conn.execute("INSERT INTO sales_wide VALUES ('n', 1, 2, 3)")
    conn.execute(
        "CREATE TABLE costs_wide ("
        "region VARCHAR, cost_q1 DOUBLE, cost_q2 DOUBLE, cost_q3 DOUBLE, cost_q4 DOUBLE)"
    )
    conn.execute("INSERT INTO costs_wide VALUES ('n', 1, 2, 3, 4)")
    conn.close()
    (tmp_path / ".env").write_text(
        f"LLM_PROVIDER=ollama\nOLLAMA_MODEL=qwen2.5:7b\nDB_MODE=duckdb\nDB_PATH={db_path}\n",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["tidy", "view", "--project-dir", str(tmp_path), "--table", "sales_wide"]
    )
    assert result.exit_code == 0, result.output
    conn = duckdb.connect(str(db_path))
    names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    conn.close()
    assert "sales_wide_long" in names
    assert "costs_wide_long" not in names


def test_tidy_suggest_accepts_csv_without_project(tmp_path):
    csv_path = tmp_path / "monthly_generation.csv"
    csv_path.write_text(
        "plant_id,fuel_type,jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec\n"
        "1,coal,180,165,140,120,110,100,95,105,130,160,175,200\n"
        "2,gas,220,200,180,170,160,175,200,220,210,200,215,230\n",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["tidy", "suggest", str(csv_path), "--format", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["table_count"] == 1
    assert len(data["suggestions"]) == 1
    s = data["suggestions"][0]
    assert s["dimensions"] == [{"name": "month", "kind": "date_period"}]
    assert len(s["column_mappings"]) == 12
    assert s["id_columns"] == ["plant_id", "fuel_type"]


def test_tidy_suggest_file_mode_rejects_table_filter(tmp_path):
    csv_path = tmp_path / "monthly.csv"
    csv_path.write_text(
        "plant_id,jan,feb,mar,apr\n1,1,2,3,4\n",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["tidy", "suggest", str(csv_path), "--table", "anything"])
    assert result.exit_code != 0
    # Click 8.3 wraps usage errors in a Rich panel with ANSI styling that
    # interpolates codes inside option names, so check the ANSI-stripped form.
    plain = re.sub(r"\x1b\[[0-9;]*m", "", result.output)
    assert "--table cannot be combined" in plain


def test_tidy_suggest_no_suggestions_message(tmp_path):
    db_path = tmp_path / "clean.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE generation ("
        "plant_id INTEGER, report_date DATE, fuel VARCHAR, net_generation_mwh DOUBLE)"
    )
    conn.close()
    (tmp_path / ".env").write_text(
        f"LLM_PROVIDER=ollama\nOLLAMA_MODEL=qwen2.5:7b\nDB_MODE=duckdb\nDB_PATH={db_path}\n",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["tidy", "suggest", "--project-dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "No untidy column-shape patterns detected" in result.output
