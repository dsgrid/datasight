"""Tests for `datasight tidy review` — plan format, apply pipeline, and CLI.

Covers the no-LLM portions of the feature:
  - Plan parsing (load_plan, structural validation).
  - Apply pipeline (apply_proposal, transactional verify-then-dispose).
  - The `tidy review --from` CLI path with single-pivot, multi-pivot, and
    every source-disposition variant.
  - `tidy review --out` round-trip from the deterministic detector.

The LLM-augmented path is exercised separately in test_tidy_review_llm.py.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from datasight.cli import cli
from datasight.tidy import ColumnMapping, Dimension, TidySuggestion
from datasight.tidy_review import (
    PLAN_VERSION,
    SourceDisposition,
    apply_proposal,
    dump_plan,
    load_plan,
    resolve_source_disposition,
    validate_against_schema,
)

from tests._env_helpers import DATASIGHT_ENV_VARS, scrub_datasight_env


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in DATASIGHT_ENV_VARS:
        monkeypatch.delenv(key, raising=False)
    yield
    scrub_datasight_env()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _build_single_pivot_db(db_path: Path) -> None:
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


def _build_multi_pivot_db(db_path: Path) -> None:
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
    conn.close()


def _project_with_db(tmp_path: Path, db_path: Path) -> str:
    (tmp_path / ".env").write_text(
        f"LLM_PROVIDER=ollama\nOLLAMA_MODEL=qwen2.5:7b\nDB_MODE=duckdb\nDB_PATH={db_path}\n",
        encoding="utf-8",
    )
    return str(tmp_path)


def _single_pivot_plan_dict() -> dict:
    return {
        "version": PLAN_VERSION,
        "proposals": [
            {
                "pattern": "repeated_prefix_period",
                "table": "sales_wide",
                "dimensions": [{"name": "year", "kind": "date_period"}],
                "id_columns": ["region"],
                "value_column": "sales",
                "target_object_name": "sales_long",
                "column_mappings": [
                    {"column": "sales_2020", "dimension_values": {"year": "2020"}},
                    {"column": "sales_2021", "dimension_values": {"year": "2021"}},
                    {"column": "sales_2022", "dimension_values": {"year": "2022"}},
                    {"column": "sales_2023", "dimension_values": {"year": "2023"}},
                ],
                "confidence": "high",
                "source": "user",
                "rationale": "test fixture",
            }
        ],
    }


def _multi_pivot_plan_dict() -> dict:
    return {
        "version": PLAN_VERSION,
        "proposals": [
            {
                "pattern": "user_proposed",
                "table": "generation_wide",
                "dimensions": [
                    {"name": "fuel_type", "kind": "category"},
                    {"name": "year", "kind": "date_period"},
                ],
                "id_columns": ["plant_id"],
                "value_column": "net_generation_mwh",
                "target_object_name": "generation_long",
                "column_mappings": [
                    {
                        "column": "coal_2020",
                        "dimension_values": {"fuel_type": "coal", "year": "2020"},
                    },
                    {
                        "column": "coal_2021",
                        "dimension_values": {"fuel_type": "coal", "year": "2021"},
                    },
                    {
                        "column": "gas_2020",
                        "dimension_values": {"fuel_type": "gas", "year": "2020"},
                    },
                    {
                        "column": "gas_2021",
                        "dimension_values": {"fuel_type": "gas", "year": "2021"},
                    },
                ],
                "confidence": "medium",
                "source": "llm",
                "rationale": "fuel-type-as-column with year suffix",
            }
        ],
    }


def _write_plan(tmp_path: Path, payload: dict) -> Path:
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Plan parsing — happy paths and structural validation
# ---------------------------------------------------------------------------


def test_load_plan_single_pivot_round_trips(tmp_path):
    plan_path = _write_plan(tmp_path, _single_pivot_plan_dict())
    plan = load_plan(plan_path)
    assert plan.version == PLAN_VERSION
    assert len(plan.proposals) == 1
    s = plan.proposals[0]
    assert s.table == "sales_wide"
    assert [d.name for d in s.dimensions] == ["year"]
    assert [m.column for m in s.column_mappings] == [
        "sales_2020",
        "sales_2021",
        "sales_2022",
        "sales_2023",
    ]
    assert s.id_columns == ["region"]
    assert s.value_column == "sales"
    assert s.target_object_name == "sales_long"
    # reshape_sql is rebuilt from the structured fields, not stored in the plan
    assert "UNPIVOT" in s.reshape_sql


def test_load_plan_multi_pivot_round_trips(tmp_path):
    plan_path = _write_plan(tmp_path, _multi_pivot_plan_dict())
    plan = load_plan(plan_path)
    s = plan.proposals[0]
    assert [d.name for d in s.dimensions] == ["fuel_type", "year"]
    assert s.column_mappings[0].dimension_values == {"fuel_type": "coal", "year": "2020"}
    # Multi-pivot must round-trip through the UNION ALL path.
    body = "\n".join(
        line for line in s.reshape_sql.splitlines() if not line.lstrip().startswith("--")
    )
    assert "UNION ALL" in body
    assert "UNPIVOT" not in body


def test_load_plan_rejects_unknown_version(tmp_path):
    payload = _single_pivot_plan_dict()
    payload["version"] = 99
    plan_path = _write_plan(tmp_path, payload)
    with pytest.raises(ValueError, match="Unsupported plan version"):
        load_plan(plan_path)


def test_load_plan_rejects_overlap_between_id_and_mapping(tmp_path):
    payload = _single_pivot_plan_dict()
    payload["proposals"][0]["id_columns"] = ["region", "sales_2020"]
    plan_path = _write_plan(tmp_path, payload)
    with pytest.raises(ValueError, match="overlap column_mappings"):
        load_plan(plan_path)


def test_load_plan_rejects_dimension_value_mismatch(tmp_path):
    payload = _multi_pivot_plan_dict()
    # Drop a dimension key in one mapping — it should now have only "fuel_type".
    payload["proposals"][0]["column_mappings"][0]["dimension_values"] = {"fuel_type": "coal"}
    plan_path = _write_plan(tmp_path, payload)
    with pytest.raises(ValueError, match="dimension_values"):
        load_plan(plan_path)


def test_load_plan_rejects_duplicate_value_tuple(tmp_path):
    payload = _multi_pivot_plan_dict()
    # Two mappings with the same (fuel_type, year) — a real-world authoring bug.
    payload["proposals"][0]["column_mappings"][1]["dimension_values"] = {
        "fuel_type": "coal",
        "year": "2020",
    }
    plan_path = _write_plan(tmp_path, payload)
    with pytest.raises(ValueError, match="duplicate dimension-value tuple"):
        load_plan(plan_path)


def test_load_plan_rejects_too_few_mappings(tmp_path):
    payload = _single_pivot_plan_dict()
    payload["proposals"][0]["column_mappings"] = payload["proposals"][0]["column_mappings"][:1]
    plan_path = _write_plan(tmp_path, payload)
    with pytest.raises(ValueError, match="at least 2 entries"):
        load_plan(plan_path)


def test_load_plan_rejects_unknown_dimension_kind(tmp_path):
    payload = _single_pivot_plan_dict()
    payload["proposals"][0]["dimensions"][0]["kind"] = "made_up_kind"
    plan_path = _write_plan(tmp_path, payload)
    with pytest.raises(ValueError, match="kind"):
        load_plan(plan_path)


def test_load_plan_rejects_invalid_confidence(tmp_path):
    payload = _single_pivot_plan_dict()
    payload["proposals"][0]["confidence"] = "extreme"
    plan_path = _write_plan(tmp_path, payload)
    with pytest.raises(ValueError, match="confidence"):
        load_plan(plan_path)


def test_dump_plan_round_trips(tmp_path):
    """A plan written by ``dump_plan`` must load back unchanged via ``load_plan``."""
    s = TidySuggestion(
        pattern="user_proposed",
        table="t",
        dimensions=[Dimension(name="year", kind="date_period")],
        column_mappings=[
            ColumnMapping(column="v_2020", dimension_values={"year": "2020"}),
            ColumnMapping(column="v_2021", dimension_values={"year": "2021"}),
        ],
        id_columns=["id"],
        value_column="v",
        target_object_name="t_long",
        rationale="round-trip",
        reshape_sql="<derived>",
    )
    out = tmp_path / "plan.json"
    dump_plan([s], out)
    loaded = load_plan(out)
    [reloaded] = loaded.proposals
    assert reloaded.table == s.table
    assert [d.name for d in reloaded.dimensions] == ["year"]
    assert [m.column for m in reloaded.column_mappings] == ["v_2020", "v_2021"]
    assert reloaded.value_column == "v"
    assert reloaded.target_object_name == "t_long"


# ---------------------------------------------------------------------------
# Schema cross-validation
# ---------------------------------------------------------------------------


def _schema_for_sales_wide() -> list[dict]:
    return [
        {
            "name": "sales_wide",
            "row_count": 2,
            "columns": [
                {"name": "region", "dtype": "VARCHAR", "nullable": True},
                {"name": "sales_2020", "dtype": "DOUBLE", "nullable": True},
                {"name": "sales_2021", "dtype": "DOUBLE", "nullable": True},
                {"name": "sales_2022", "dtype": "DOUBLE", "nullable": True},
                {"name": "sales_2023", "dtype": "DOUBLE", "nullable": True},
            ],
        }
    ]


def test_validate_against_schema_clean(tmp_path):
    plan = load_plan(_write_plan(tmp_path, _single_pivot_plan_dict()))
    problems = validate_against_schema(plan.proposals[0], _schema_for_sales_wide())
    assert problems == []


def test_validate_against_schema_missing_column(tmp_path):
    payload = _single_pivot_plan_dict()
    payload["proposals"][0]["column_mappings"].append(
        {"column": "sales_2099", "dimension_values": {"year": "2099"}}
    )
    plan = load_plan(_write_plan(tmp_path, payload))
    problems = validate_against_schema(plan.proposals[0], _schema_for_sales_wide())
    assert any("sales_2099" in p for p in problems)


def test_validate_against_schema_missing_table(tmp_path):
    plan = load_plan(_write_plan(tmp_path, _single_pivot_plan_dict()))
    schema_no_match = [{"name": "other", "row_count": 0, "columns": []}]
    problems = validate_against_schema(plan.proposals[0], schema_no_match)
    assert problems == ["source table 'sales_wide' not found in database"]


def test_validate_against_schema_target_collision(tmp_path):
    plan = load_plan(_write_plan(tmp_path, _single_pivot_plan_dict()))
    schema = _schema_for_sales_wide() + [{"name": "sales_long", "row_count": 0, "columns": []}]
    problems = validate_against_schema(plan.proposals[0], schema)
    assert any("already exists" in p for p in problems)


# ---------------------------------------------------------------------------
# Apply pipeline (single-pivot, multi-pivot, dispositions)
# ---------------------------------------------------------------------------


def test_apply_proposal_single_pivot_table_keep(tmp_path):
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    plan = load_plan(_write_plan(tmp_path, _single_pivot_plan_dict()))
    conn = duckdb.connect(str(db_path))
    try:
        result = apply_proposal(
            conn,
            plan.proposals[0],
            mode="table",
            source_disposition=SourceDisposition(mode="keep"),
            dry_run=False,
        )
        assert result.row_count_source == 2
        assert result.row_count_target == 8
        assert result.source_disposition == "keep"
        assert result.dry_run is False
        # Source must be untouched.
        rows = conn.execute("SELECT COUNT(*) FROM sales_wide").fetchone()
        assert rows is not None and rows[0] == 2
        # Target carries the long form.
        rows = conn.execute(
            "SELECT region, year, sales FROM sales_long ORDER BY region, year"
        ).fetchall()
    finally:
        conn.close()
    assert rows[0] == ("north", "2020", 10.0)
    assert len(rows) == 8


def test_apply_proposal_single_pivot_view_keep(tmp_path):
    """View mode must use UNION ALL (duckdb 1.5.2 view-binding bug) and survive
    being queried back through the Python binding."""
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    plan = load_plan(_write_plan(tmp_path, _single_pivot_plan_dict()))
    conn = duckdb.connect(str(db_path))
    try:
        apply_proposal(
            conn,
            plan.proposals[0],
            mode="view",
            source_disposition=SourceDisposition(mode="keep"),
            dry_run=False,
        )
    finally:
        conn.close()
    # Reopen on a fresh connection — that's where the duckdb 1.5.2 binder bug
    # used to surface for UNPIVOT-inside-views. UNION ALL must survive.
    conn = duckdb.connect(str(db_path))
    try:
        rows = conn.execute("SELECT COUNT(*) FROM sales_long").fetchone()
        assert rows is not None and rows[0] == 8
        kind = conn.execute(
            "SELECT table_type FROM information_schema.tables WHERE table_name = 'sales_long'"
        ).fetchone()
        assert kind is not None and kind[0] == "VIEW"
    finally:
        conn.close()


def test_apply_proposal_multi_pivot_table_keep(tmp_path):
    db_path = tmp_path / "multi.duckdb"
    _build_multi_pivot_db(db_path)
    plan = load_plan(_write_plan(tmp_path, _multi_pivot_plan_dict()))
    conn = duckdb.connect(str(db_path))
    try:
        result = apply_proposal(
            conn,
            plan.proposals[0],
            mode="table",
            source_disposition=SourceDisposition(mode="keep"),
            dry_run=False,
        )
        assert result.row_count_source == 2
        assert result.row_count_target == 8
        rows = conn.execute(
            "SELECT plant_id, fuel_type, year, net_generation_mwh "
            "FROM generation_long ORDER BY plant_id, fuel_type, year"
        ).fetchall()
    finally:
        conn.close()
    assert rows[0] == (1, "coal", "2020", 100.0)
    assert len(rows) == 8


def test_apply_proposal_dry_run_does_not_change_database(tmp_path):
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    plan = load_plan(_write_plan(tmp_path, _single_pivot_plan_dict()))
    conn = duckdb.connect(str(db_path))
    try:
        result = apply_proposal(
            conn,
            plan.proposals[0],
            mode="table",
            source_disposition=SourceDisposition(mode="drop"),
            dry_run=True,
        )
        assert result.dry_run is True
        # Counts are predicted, not observed.
        assert result.row_count_target == 8
        # Database is unchanged: source is still there, no long form yet.
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert "sales_wide" in names
    assert "sales_long" not in names


def test_apply_proposal_rename_source(tmp_path):
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    plan = load_plan(_write_plan(tmp_path, _single_pivot_plan_dict()))
    conn = duckdb.connect(str(db_path))
    try:
        result = apply_proposal(
            conn,
            plan.proposals[0],
            mode="table",
            source_disposition=SourceDisposition(mode="rename", new_name="sales_wide_raw"),
            dry_run=False,
        )
        assert result.source_renamed_to == "sales_wide_raw"
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert "sales_wide" not in names
    assert "sales_wide_raw" in names
    assert "sales_long" in names


def test_apply_proposal_drop_source(tmp_path):
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    plan = load_plan(_write_plan(tmp_path, _single_pivot_plan_dict()))
    conn = duckdb.connect(str(db_path))
    try:
        apply_proposal(
            conn,
            plan.proposals[0],
            mode="table",
            source_disposition=SourceDisposition(mode="drop"),
            dry_run=False,
        )
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert "sales_wide" not in names
    assert "sales_long" in names


def test_apply_proposal_verification_failure_rolls_back(tmp_path):
    """If the verify step fails (e.g., id_columns omits a duplicating column),
    the transaction must roll back so the database is unchanged."""
    db_path = tmp_path / "dup.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE dup_wide (region VARCHAR, v_2020 DOUBLE, v_2021 DOUBLE, v_2022 DOUBLE)"
    )
    # Two rows with the same `region` — if the plan omits `region` from
    # id_columns, the long form will have 6 rows, not 6 expected... so we
    # need a stronger trigger. Use a plan that drops a non-redundant id.
    conn.execute("INSERT INTO dup_wide VALUES ('north', 1, 2, 3), ('north', 10, 20, 30)")
    conn.close()

    # Plan deliberately omits `region` from id_columns. Source has 2 rows × 3
    # mapped columns = expected 6. The reshape will produce 6 too, so to force
    # a real mismatch we need a plan that genuinely under/over-counts. Use a
    # bad target name collision check instead — call the verify path with a
    # plan whose mapping count claims 4 columns but we only execute 3.
    class _UnderCountingSuggestion(TidySuggestion):
        """A suggestion whose declared column_mappings claim more columns than
        the hand-crafted DDL actually emits. Drives apply_proposal's verify
        step into the mismatch branch so we can pin the rollback behavior."""

        def build_sql(self, mode: str = "table") -> str:
            return self.reshape_sql

    suggestion = _UnderCountingSuggestion(
        pattern="user_proposed",
        table="dup_wide",
        dimensions=[Dimension(name="year", kind="date_period")],
        column_mappings=[
            ColumnMapping(column="v_2020", dimension_values={"year": "2020"}),
            ColumnMapping(column="v_2021", dimension_values={"year": "2021"}),
            ColumnMapping(column="v_2022", dimension_values={"year": "2022"}),
            # Phantom mapping — column doesn't actually appear in the DDL
            # below, so the declared mapping count (4) overshoots the rows
            # the DDL produces (6 = 3 cols × 2 rows). Verify expects 8.
            ColumnMapping(column="v_phantom", dimension_values={"year": "phantom"}),
        ],
        id_columns=["region"],
        value_column="v",
        target_object_name="dup_long",
        rationale="force a verify mismatch",
        reshape_sql=(
            "CREATE OR REPLACE TABLE dup_long AS "
            "SELECT region, '2020' AS year, v_2020 AS v FROM dup_wide "
            "UNION ALL SELECT region, '2021', v_2021 FROM dup_wide "
            "UNION ALL SELECT region, '2022', v_2022 FROM dup_wide;"
        ),
    )

    conn = duckdb.connect(str(db_path))
    try:
        with pytest.raises(RuntimeError, match="verification failed"):
            apply_proposal(
                conn,
                suggestion,
                mode="table",
                source_disposition=SourceDisposition(mode="drop"),
                dry_run=False,
            )
        # Database must be unchanged: source still there, no long form, no drop.
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert "dup_wide" in names
    assert "dup_long" not in names


# ---------------------------------------------------------------------------
# Source disposition flag resolution
# ---------------------------------------------------------------------------


def test_resolve_source_disposition_default_keep():
    d = resolve_source_disposition(keep=False, rename_to=None, drop=False)
    assert d.mode == "keep"


def test_resolve_source_disposition_explicit_keep():
    d = resolve_source_disposition(keep=True, rename_to=None, drop=False)
    assert d.mode == "keep"


def test_resolve_source_disposition_rename():
    d = resolve_source_disposition(keep=False, rename_to="raw_table", drop=False)
    assert d.mode == "rename"
    assert d.new_name == "raw_table"


def test_resolve_source_disposition_drop():
    d = resolve_source_disposition(keep=False, rename_to=None, drop=True)
    assert d.mode == "drop"


def test_resolve_source_disposition_rejects_combination():
    with pytest.raises(ValueError, match="mutually exclusive"):
        resolve_source_disposition(keep=False, rename_to="x", drop=True)
    with pytest.raises(ValueError, match="mutually exclusive"):
        resolve_source_disposition(keep=True, rename_to="x", drop=False)


# ---------------------------------------------------------------------------
# `datasight tidy review` — CLI
# ---------------------------------------------------------------------------


def test_cli_review_from_plan_apply_all_keep_source(tmp_path):
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    project_dir = _project_with_db(tmp_path, db_path)
    plan_path = _write_plan(tmp_path, _single_pivot_plan_dict())
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "tidy",
            "review",
            "--project-dir",
            project_dir,
            "--from",
            str(plan_path),
            "--apply-all",
            "--as",
            "table",
        ],
    )
    assert result.exit_code == 0, result.output
    conn = duckdb.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT region, year, sales FROM sales_long ORDER BY region, year"
        ).fetchall()
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert len(rows) == 8
    assert "sales_wide" in names  # default disposition is keep


def test_cli_review_from_plan_drop_source(tmp_path):
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    project_dir = _project_with_db(tmp_path, db_path)
    plan_path = _write_plan(tmp_path, _single_pivot_plan_dict())
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "tidy",
            "review",
            "--project-dir",
            project_dir,
            "--from",
            str(plan_path),
            "--apply-all",
            "--as",
            "table",
            "--drop-source",
        ],
    )
    assert result.exit_code == 0, result.output
    conn = duckdb.connect(str(db_path))
    try:
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert "sales_wide" not in names
    assert "sales_long" in names


def test_cli_review_from_plan_rename_source(tmp_path):
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    project_dir = _project_with_db(tmp_path, db_path)
    plan_path = _write_plan(tmp_path, _single_pivot_plan_dict())
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "tidy",
            "review",
            "--project-dir",
            project_dir,
            "--from",
            str(plan_path),
            "--apply-all",
            "--as",
            "table",
            "--rename-source",
            "sales_wide_raw",
        ],
    )
    assert result.exit_code == 0, result.output
    conn = duckdb.connect(str(db_path))
    try:
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert "sales_wide" not in names
    assert "sales_wide_raw" in names
    assert "sales_long" in names


def test_cli_review_disposition_flags_are_mutex(tmp_path):
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    project_dir = _project_with_db(tmp_path, db_path)
    plan_path = _write_plan(tmp_path, _single_pivot_plan_dict())
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "tidy",
            "review",
            "--project-dir",
            project_dir,
            "--from",
            str(plan_path),
            "--apply-all",
            "--drop-source",
            "--rename-source",
            "x",
        ],
    )
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output


def test_cli_review_dry_run_emits_ddl_and_does_not_apply(tmp_path):
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    project_dir = _project_with_db(tmp_path, db_path)
    plan_path = _write_plan(tmp_path, _single_pivot_plan_dict())
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "tidy",
            "review",
            "--project-dir",
            project_dir,
            "--from",
            str(plan_path),
            "--apply-all",
            "--dry-run",
            "--as",
            "table",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Would apply" in result.output
    assert "CREATE OR REPLACE TABLE" in result.output
    conn = duckdb.connect(str(db_path))
    try:
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert "sales_long" not in names


def test_cli_review_multi_pivot_from_plan(tmp_path):
    db_path = tmp_path / "multi.duckdb"
    _build_multi_pivot_db(db_path)
    project_dir = _project_with_db(tmp_path, db_path)
    plan_path = _write_plan(tmp_path, _multi_pivot_plan_dict())
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "tidy",
            "review",
            "--project-dir",
            project_dir,
            "--from",
            str(plan_path),
            "--apply-all",
            "--as",
            "table",
        ],
    )
    assert result.exit_code == 0, result.output
    conn = duckdb.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT plant_id, fuel_type, year, net_generation_mwh "
            "FROM generation_long ORDER BY plant_id, fuel_type, year"
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 8
    assert rows[0] == (1, "coal", "2020", 100.0)


def test_cli_review_out_dumps_deterministic_hits(tmp_path):
    """`tidy review --out` without `--from` should write the deterministic
    detector's hits as a plan file — a starting point the developer can edit
    and feed back via `--from`."""
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    project_dir = _project_with_db(tmp_path, db_path)
    out_path = tmp_path / "detector.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "tidy",
            "review",
            "--project-dir",
            project_dir,
            "--out",
            str(out_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert out_path.exists()
    plan = load_plan(out_path)
    assert len(plan.proposals) == 1
    assert plan.proposals[0].table == "sales_wide"
    assert plan.proposals[0].source == "deterministic"


def test_cli_review_without_apply_all_errors_until_interactive_lands(tmp_path):
    """The interactive prompt loop is task 4. Until it's wired, omitting
    --apply-all must surface a clear UsageError pointing at --apply-all
    or --dry-run."""
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    project_dir = _project_with_db(tmp_path, db_path)
    plan_path = _write_plan(tmp_path, _single_pivot_plan_dict())
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "tidy",
            "review",
            "--project-dir",
            project_dir,
            "--from",
            str(plan_path),
        ],
    )
    assert result.exit_code != 0
    assert "Interactive review" in result.output


def test_cli_review_invalid_plan_against_schema(tmp_path):
    """A plan that references a column that doesn't exist must be rejected
    cleanly before any DDL runs."""
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    project_dir = _project_with_db(tmp_path, db_path)
    payload = _single_pivot_plan_dict()
    payload["proposals"][0]["column_mappings"].append(
        {"column": "sales_2099", "dimension_values": {"year": "2099"}}
    )
    plan_path = _write_plan(tmp_path, payload)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "tidy",
            "review",
            "--project-dir",
            project_dir,
            "--from",
            str(plan_path),
            "--apply-all",
            "--as",
            "table",
        ],
    )
    assert result.exit_code != 0
    assert "sales_2099" in result.output
    conn = duckdb.connect(str(db_path))
    try:
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert "sales_long" not in names  # nothing applied


def test_cli_review_table_filter_scopes_apply(tmp_path):
    """`--table` should restrict apply to proposals whose `table` matches."""
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    _build_multi_pivot_db(db_path)  # appends generation_wide into the same file
    project_dir = _project_with_db(tmp_path, db_path)
    payload = {
        "version": PLAN_VERSION,
        "proposals": [
            _single_pivot_plan_dict()["proposals"][0],
            _multi_pivot_plan_dict()["proposals"][0],
        ],
    }
    plan_path = _write_plan(tmp_path, payload)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "tidy",
            "review",
            "--project-dir",
            project_dir,
            "--from",
            str(plan_path),
            "--apply-all",
            "--as",
            "table",
            "--table",
            "sales_wide",
        ],
    )
    assert result.exit_code == 0, result.output
    conn = duckdb.connect(str(db_path))
    try:
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert "sales_long" in names
    assert "generation_long" not in names
