"""Tests for `datasight tidy review` — plan format, apply pipeline, LLM,
and the interactive prompt loop.

Covers:
  - Plan parsing (load_plan, structural validation).
  - Apply pipeline (apply_proposal, transactional verify-then-dispose).
  - The `tidy review --from` CLI path with single-pivot, multi-pivot, and
    every source-disposition variant.
  - `tidy review --out` round-trip from the deterministic detector.
  - LLM proposal parsing and the LLM call wrapper, exercised with a
    FakeLLMClient so tests are deterministic and offline.
  - Interactive prompt loop: skip, apply, edit, quit.

A live LLM integration test against Ollama lives at the bottom under the
`integration` mark and is skipped in CI.
"""

from __future__ import annotations

import json
import re
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
    update_schema_yaml_for_apply,
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


def _build_single_pivot_view_db(db_path: Path, csv_path: Path) -> None:
    """Like ``_build_single_pivot_db`` but ``sales_wide`` is a view over a CSV.

    Mirrors the real-world setup the user reported: ``datasight init`` /
    ``generate`` may register a CSV-backed view, and ``tidy review
    --drop-source`` then has to drop a view rather than a table.
    """
    csv_path.write_text(
        "region,sales_2020,sales_2021,sales_2022,sales_2023\n"
        "north,10,20,30,40\n"
        "south,5,15,25,35\n",
        encoding="utf-8",
    )
    conn = duckdb.connect(str(db_path))
    conn.execute(f"CREATE VIEW sales_wide AS SELECT * FROM read_csv_auto('{csv_path.as_posix()}')")
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


def test_apply_proposal_replace_source(tmp_path):
    """``replace`` drops the original wide table and renames the long form
    into its place. Downstream consumers continue to query the same table
    name; only the shape (and the data) has changed. (Previously this
    behavior was the ``drop`` mode; the rename was prompted by the user-
    facing label being more accurate.)"""
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    plan = load_plan(_write_plan(tmp_path, _single_pivot_plan_dict()))
    conn = duckdb.connect(str(db_path))
    try:
        result = apply_proposal(
            conn,
            plan.proposals[0],
            mode="table",
            source_disposition=SourceDisposition(mode="replace"),
            dry_run=False,
        )
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        # The long form must answer queries by the source's old name.
        rows = conn.execute(
            "SELECT region, year, sales FROM sales_wide ORDER BY region, year"
        ).fetchall()
    finally:
        conn.close()
    assert "sales_wide" in names
    assert "sales_long" not in names  # renamed away
    assert result.final_target_name == "sales_wide"
    assert len(rows) == 8
    assert rows[0] == ("north", "2020", 10.0)


def test_apply_proposal_drop_source_keeps_long_form_name(tmp_path):
    """``drop`` is the bare-drop semantics: the source goes away but the
    long form keeps its target name. Downstream code that referenced the
    source name will fail; pick this when the new shape is the canonical
    one going forward."""
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
            dry_run=False,
        )
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert "sales_wide" not in names
    assert "sales_long" in names  # kept its target name
    assert result.final_target_name == "sales_long"
    assert result.source_disposition == "drop"


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
        # The strict ``count == source × mapped`` verify only fires for
        # include_nulls=True; the bare-drop case relies on a looser range
        # check that wouldn't trip on this synthetic mismatch.
        include_nulls=True,
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
    d = resolve_source_disposition(keep=False, rename_to=None, replace=False, drop=False)
    assert d.mode == "keep"


def test_resolve_source_disposition_explicit_keep():
    d = resolve_source_disposition(keep=True, rename_to=None, replace=False, drop=False)
    assert d.mode == "keep"


def test_resolve_source_disposition_rename():
    d = resolve_source_disposition(keep=False, rename_to="raw_table", replace=False, drop=False)
    assert d.mode == "rename"
    assert d.new_name == "raw_table"


def test_resolve_source_disposition_replace():
    """``--replace-source`` carries the original ``--drop-source`` semantics:
    drop the source, long form takes over its name."""
    d = resolve_source_disposition(keep=False, rename_to=None, replace=True, drop=False)
    assert d.mode == "replace"


def test_resolve_source_disposition_drop():
    """``--drop-source`` is the bare drop: source goes away, long form keeps
    its target name. (Breaking change from the prior CLI semantics.)"""
    d = resolve_source_disposition(keep=False, rename_to=None, replace=False, drop=True)
    assert d.mode == "drop"


def test_resolve_source_disposition_rejects_combination():
    with pytest.raises(ValueError, match="mutually exclusive"):
        resolve_source_disposition(keep=False, rename_to="x", replace=False, drop=True)
    with pytest.raises(ValueError, match="mutually exclusive"):
        resolve_source_disposition(keep=True, rename_to="x", replace=False, drop=False)
    with pytest.raises(ValueError, match="mutually exclusive"):
        resolve_source_disposition(keep=False, rename_to=None, replace=True, drop=True)


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


def test_cli_review_from_plan_replace_source(tmp_path):
    """End-to-end ``--replace-source``: source dropped, long form renamed
    to source's name. (Previously this was the ``--drop-source`` flag.)"""
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
            "--replace-source",
        ],
    )
    assert result.exit_code == 0, result.output
    conn = duckdb.connect(str(db_path))
    try:
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        kind = conn.execute(
            "SELECT table_type FROM information_schema.tables WHERE table_name = 'sales_wide'"
        ).fetchone()
    finally:
        conn.close()
    assert "sales_wide" in names  # the long form took the source's name
    assert "sales_long" not in names
    # And the surviving "sales_wide" really is the new long-form table.
    assert kind is not None and kind[0] == "BASE TABLE"


def test_cli_review_from_plan_drop_source(tmp_path):
    """``--drop-source`` after the breaking change: source goes away, long
    form keeps its target name. Downstream queries against the source name
    fail."""
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
    assert "sales_long" in names  # long form kept its target name


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


def test_cli_review_interactive_apply_then_quit(tmp_path):
    """Interactive: apply the first proposal, then quit at the second.

    The loop sees two proposals (a multi-pivot plan with two distinct tables).
    Sending "a\\nq\\n" applies the first and stops before the second. The
    early-quit path must NOT roll back the first proposal — already-applied
    work is committed and audited as it goes."""
    db_path = tmp_path / "both.duckdb"
    _build_single_pivot_db(db_path)
    _build_multi_pivot_db(db_path)
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
            "--as",
            "table",
        ],
        input="a\nq\n",
    )
    assert result.exit_code == 0, result.output
    assert "Proposal 1 of 2" in result.output
    assert "Stopped at proposal 2 of 2" in result.output
    conn = duckdb.connect(str(db_path))
    try:
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert "sales_long" in names  # proposal 1 was applied
    assert "generation_long" not in names  # proposal 2 was skipped via quit


def test_cli_review_interactive_skip_all(tmp_path):
    """Skipping every proposal must leave the database untouched and exit 0."""
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
        input="s\n",
    )
    assert result.exit_code == 0, result.output
    assert "Skipped" in result.output
    assert "No proposals approved" in result.output
    conn = duckdb.connect(str(db_path))
    try:
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert "sales_long" not in names


def test_cli_review_interactive_edit_then_apply(tmp_path):
    """Edit the target name, then apply. The edited target should land in
    the database, not the original `sales_long`."""
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
            "--as",
            "table",
        ],
        # Inputs (one per line):
        #   e          -> enter edit menu
        #   1          -> pick target_object_name
        #   sales_tidy -> new name
        #   b          -> back to main menu
        #   a          -> apply
        input="e\n1\nsales_tidy\nb\na\n",
    )
    assert result.exit_code == 0, result.output
    conn = duckdb.connect(str(db_path))
    try:
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert "sales_tidy" in names
    assert "sales_long" not in names


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


# ---------------------------------------------------------------------------
# LLM proposal parsing — tolerant validator drops bad apples, keeps survivors
# ---------------------------------------------------------------------------


def _llm_proposal(
    *,
    table: str = "fuel_wide",
    dimensions: list | None = None,
    id_columns: list | None = None,
    column_mappings: list | None = None,
    value_column: str = "net_generation_mwh",
    confidence: str = "medium",
    rationale: str = "fuel-as-column",
    target_object_name: str | None = None,
) -> dict:
    """Build a well-formed LLM proposal dict for tests."""
    proposal = {
        "table": table,
        "dimensions": dimensions or [{"name": "fuel_type", "kind": "category"}],
        "id_columns": id_columns or ["plant_id"],
        "value_column": value_column,
        "column_mappings": column_mappings
        or [
            {"column": "coal_mwh", "dimension_values": {"fuel_type": "coal"}},
            {"column": "gas_mwh", "dimension_values": {"fuel_type": "gas"}},
            {"column": "nuclear_mwh", "dimension_values": {"fuel_type": "nuclear"}},
        ],
        "confidence": confidence,
        "rationale": rationale,
    }
    if target_object_name:
        proposal["target_object_name"] = target_object_name
    return proposal


def test_parse_llm_proposals_passes_clean_proposal_through():
    from datasight.tidy_llm import parse_llm_proposals

    result = parse_llm_proposals([_llm_proposal()])
    assert len(result.suggestions) == 1
    assert result.parse_warnings == []
    s = result.suggestions[0]
    assert s.source == "llm"
    assert [d.name for d in s.dimensions] == ["fuel_type"]
    assert [m.column for m in s.column_mappings] == [
        "coal_mwh",
        "gas_mwh",
        "nuclear_mwh",
    ]


def test_parse_llm_proposals_drops_malformed_keeps_rest():
    """One malformed proposal must not torpedo the batch."""
    from datasight.tidy_llm import parse_llm_proposals

    bad = _llm_proposal(table="bad")
    bad["dimensions"][0]["kind"] = "made_up_kind"  # invalid kind
    good = _llm_proposal(table="good")
    result = parse_llm_proposals([bad, good])
    assert len(result.suggestions) == 1
    assert result.suggestions[0].table == "good"
    assert len(result.parse_warnings) == 1
    assert "bad" in result.parse_warnings[0]


def test_parse_llm_proposals_handles_multi_pivot():
    from datasight.tidy_llm import parse_llm_proposals

    proposal = _llm_proposal(
        table="fuel_year_wide",
        dimensions=[
            {"name": "fuel_type", "kind": "category"},
            {"name": "year", "kind": "date_period"},
        ],
        column_mappings=[
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
    )
    result = parse_llm_proposals([proposal])
    assert len(result.suggestions) == 1
    s = result.suggestions[0]
    assert [d.name for d in s.dimensions] == ["fuel_type", "year"]
    body = "\n".join(
        line for line in s.reshape_sql.splitlines() if not line.lstrip().startswith("--")
    )
    assert "UNION ALL" in body
    assert "UNPIVOT" not in body


def test_parse_llm_proposals_drops_overlap_between_id_and_mappings():
    from datasight.tidy_llm import parse_llm_proposals

    bad = _llm_proposal()
    bad["id_columns"] = ["plant_id", "coal_mwh"]  # overlaps mapping
    result = parse_llm_proposals([bad])
    assert result.suggestions == []
    assert any("overlap" in w for w in result.parse_warnings)


def test_parse_llm_proposals_handles_non_dict_entry():
    """Defensive: the model could in theory return something not dict-shaped."""
    from datasight.tidy_llm import parse_llm_proposals

    raw: list = ["not a dict", _llm_proposal()]
    result = parse_llm_proposals(raw)
    assert len(result.suggestions) == 1
    assert len(result.parse_warnings) == 1


# ---------------------------------------------------------------------------
# LLM call wrapper — fake LLMClient drives the structured-output path
# ---------------------------------------------------------------------------


class _FakeLLMClient:
    """Scripted LLM that returns one canned response per call.

    Mirrors the FakeLLMClient pattern in test_verify.py so tidy review
    integration tests stay self-contained and don't require a live model.
    """

    def __init__(self, responses: list):
        self._responses = list(responses)
        self.calls: list[dict] = []

    async def create_message(
        self,
        *,
        model: str,
        system: str,
        messages: list,
        tools: list,
        max_tokens: int,
    ):
        self.calls.append(
            {
                "model": model,
                "system": system,
                "messages": [dict(m) for m in messages],
                "tools": list(tools),
                "max_tokens": max_tokens,
            }
        )
        if not self._responses:
            from datasight.llm import LLMResponse, TextBlock

            return LLMResponse(content=[TextBlock(text="done")], stop_reason="end_turn")
        return self._responses.pop(0)


def _propose_response(proposals: list[dict]):
    from datasight.llm import LLMResponse, ToolUseBlock, Usage

    return LLMResponse(
        content=[
            ToolUseBlock(
                id="tu_1",
                name="propose_reshapes",
                input={"proposals": proposals},
            )
        ],
        stop_reason="tool_use",
        usage=Usage(),
    )


async def test_propose_reshapes_calls_tool_and_parses(tmp_path):
    from datasight.tidy_llm import propose_reshapes

    fake = _FakeLLMClient([_propose_response([_llm_proposal()])])
    schema_info = [
        {
            "name": "fuel_wide",
            "row_count": 5,
            "columns": [
                {"name": "plant_id", "dtype": "INTEGER", "nullable": False},
                {"name": "coal_mwh", "dtype": "DOUBLE", "nullable": True},
                {"name": "gas_mwh", "dtype": "DOUBLE", "nullable": True},
                {"name": "nuclear_mwh", "dtype": "DOUBLE", "nullable": True},
            ],
        }
    ]
    result = await propose_reshapes(
        fake,
        model="qwen2.5:7b",
        schema_info=schema_info,
        deterministic_hits=[],
    )
    assert len(result.suggestions) == 1
    assert result.suggestions[0].source == "llm"
    # The system prompt and tool went over the wire as expected.
    [call] = fake.calls
    assert call["tools"][0]["name"] == "propose_reshapes"
    assert "energy-research" in call["system"].lower()
    assert "<schema>" in call["messages"][0]["content"]
    assert "<already_detected_by_regex>" in call["messages"][0]["content"]


async def test_propose_reshapes_empty_proposals_is_valid():
    from datasight.tidy_llm import propose_reshapes

    fake = _FakeLLMClient([_propose_response([])])
    result = await propose_reshapes(
        fake,
        model="qwen2.5:7b",
        schema_info=[],
        deterministic_hits=[],
    )
    assert result.suggestions == []
    assert result.raw_proposals == []


async def test_propose_reshapes_text_only_response_is_no_op():
    """If the model returns prose without calling the tool, treat it as
    'no proposals' rather than as an error."""
    from datasight.llm import LLMResponse, TextBlock

    from datasight.tidy_llm import propose_reshapes

    fake = _FakeLLMClient(
        [LLMResponse(content=[TextBlock(text="nothing to suggest")], stop_reason="end_turn")]
    )
    result = await propose_reshapes(
        fake,
        model="qwen2.5:7b",
        schema_info=[],
        deterministic_hits=[],
    )
    assert result.suggestions == []


async def test_propose_reshapes_user_message_includes_samples():
    from datasight.tidy_llm import propose_reshapes

    fake = _FakeLLMClient([_propose_response([])])
    samples = {"fuel_wide": [{"plant_id": 1, "coal_mwh": 100.0}]}
    await propose_reshapes(
        fake,
        model="qwen2.5:7b",
        schema_info=[],
        deterministic_hits=[],
        samples=samples,
    )
    [call] = fake.calls
    content = call["messages"][0]["content"]
    assert "<samples>" in content
    assert "coal_mwh" in content


def test_system_prompt_pins_value_column_to_value():
    """The system prompt must steer the LLM toward ``value`` as the value
    column name. Without this rule the model picks domain-specific names
    like ``load_mwh`` per table, which makes downstream queries
    inconsistent across reshapes."""
    from datasight.tidy_llm import PROPOSE_RESHAPES_TOOL, SYSTEM_PROMPT

    # Prompt-level rule.
    assert "value column" in SYSTEM_PROMPT.lower()
    assert "always use `value`" in SYSTEM_PROMPT.lower()
    # Tool-schema rule (belt-and-braces — model weighs both).
    value_desc = PROPOSE_RESHAPES_TOOL["input_schema"]["properties"]["proposals"]["items"][
        "properties"
    ]["value_column"]["description"]
    assert "default to 'value'" in value_desc.lower()
    assert "do not bake units" in value_desc.lower()


# ---------------------------------------------------------------------------
# CLI review with a fake LLMClient injected via monkeypatch
# ---------------------------------------------------------------------------


def test_cli_review_llm_path_with_fake_client(tmp_path, monkeypatch):
    """End-to-end CLI test of the LLM path with a fake client.

    Wires through `cli.create_llm_client` so the test doesn't hit a real
    provider, then verifies the LLM-proposed suggestion gets applied.
    """
    db_path = tmp_path / "fuel.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE fuel_wide ("
        "plant_id INTEGER, "
        "coal_mwh DOUBLE, gas_mwh DOUBLE, nuclear_mwh DOUBLE)"
    )
    conn.execute("INSERT INTO fuel_wide VALUES (1, 100, 200, 300), (2, 50, 150, 250)")
    conn.close()
    project_dir = _project_with_db(tmp_path, db_path)

    proposal = _llm_proposal(
        table="fuel_wide",
        column_mappings=[
            {"column": "coal_mwh", "dimension_values": {"fuel_type": "coal"}},
            {"column": "gas_mwh", "dimension_values": {"fuel_type": "gas"}},
            {"column": "nuclear_mwh", "dimension_values": {"fuel_type": "nuclear"}},
        ],
        target_object_name="fuel_long",
    )
    fake = _FakeLLMClient([_propose_response([proposal])])

    def _make_fake(**_kwargs):
        return fake

    monkeypatch.setattr("datasight.cli.create_llm_client", _make_fake)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "tidy",
            "review",
            "--project-dir",
            project_dir,
            "--apply-all",
            "--as",
            "table",
        ],
    )
    assert result.exit_code == 0, result.output
    conn = duckdb.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT plant_id, fuel_type, net_generation_mwh "
            "FROM fuel_long ORDER BY plant_id, fuel_type"
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 6  # 2 source rows × 3 mapped columns
    assert rows[0] == (1, "coal", 100.0)
    # The fake LLM was called once.
    assert len(fake.calls) == 1


# ---------------------------------------------------------------------------
# Live LLM integration test — skipped in CI
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_propose_reshapes_live_llm_proposes_fuel_pivot():
    """Smoke-test against a real local Ollama model. The deterministic
    detector cannot recognize a fuel-type-as-column pivot; the LLM must.

    Asserts only on `column_mappings.column` (the structural fact), not
    on dimension_name or rationale text — those are LLM-stylistic choices
    that drift between model versions."""
    from datasight.llm import create_llm_client
    from datasight.tidy_llm import propose_reshapes

    schema_info = [
        {
            "name": "generation_fuel_wide",
            "row_count": 100,
            "columns": [
                {"name": "plant_id", "dtype": "INTEGER", "nullable": False},
                {"name": "report_date", "dtype": "DATE", "nullable": False},
                {"name": "coal_mwh", "dtype": "DOUBLE", "nullable": True},
                {"name": "gas_mwh", "dtype": "DOUBLE", "nullable": True},
                {"name": "nuclear_mwh", "dtype": "DOUBLE", "nullable": True},
                {"name": "solar_mwh", "dtype": "DOUBLE", "nullable": True},
                {"name": "wind_mwh", "dtype": "DOUBLE", "nullable": True},
            ],
        }
    ]
    client = create_llm_client(
        provider="ollama",
        model="qwen2.5:7b",
        api_key="",
        base_url="http://localhost:11434",
        timeout=120,
    )
    result = await propose_reshapes(
        client,
        model="qwen2.5:7b",
        schema_info=schema_info,
        deterministic_hits=[],
    )
    assert len(result.suggestions) >= 1
    fuel_columns = {"coal_mwh", "gas_mwh", "nuclear_mwh", "solar_mwh", "wind_mwh"}
    proposed_columns = {m.column for s in result.suggestions for m in s.column_mappings}
    # The LLM should propose pivoting at least three of the five fuel columns.
    assert len(fuel_columns & proposed_columns) >= 3


# ---------------------------------------------------------------------------
# View sources — DROP / ALTER must use the right keyword (DuckDB rejects
# `DROP TABLE <view>` / `ALTER TABLE <view>` with a binder error).
# ---------------------------------------------------------------------------


def test_apply_proposal_replace_source_when_source_is_view(tmp_path):
    """``replace`` must succeed when the source is a CSV-backed view.

    Reproduces the user-reported failure: a view (typical when a project is
    set up by registering CSV files) cannot be dropped with `DROP TABLE`.
    The apply flow must detect the kind and emit `DROP VIEW` / `ALTER VIEW`.
    """
    db_path = tmp_path / "wide.duckdb"
    csv_path = tmp_path / "sales.csv"
    _build_single_pivot_view_db(db_path, csv_path)
    plan = load_plan(_write_plan(tmp_path, _single_pivot_plan_dict()))
    conn = duckdb.connect(str(db_path))
    try:
        result = apply_proposal(
            conn,
            plan.proposals[0],
            mode="table",
            source_disposition=SourceDisposition(mode="replace"),
            dry_run=False,
        )
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        kind = conn.execute(
            "SELECT table_type FROM information_schema.tables WHERE table_name = 'sales_wide'"
        ).fetchone()
    finally:
        conn.close()
    assert "sales_wide" in names
    assert result.final_target_name == "sales_wide"
    # The original view is gone; the surviving `sales_wide` is the new long
    # form (a base table created by the reshape DDL).
    assert kind is not None and kind[0] == "BASE TABLE"


def test_apply_proposal_rename_source_when_source_is_view(tmp_path):
    """`--rename-source` on a view-backed source must use `ALTER VIEW`."""
    db_path = tmp_path / "wide.duckdb"
    csv_path = tmp_path / "sales.csv"
    _build_single_pivot_view_db(db_path, csv_path)
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
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        kinds = dict(
            conn.execute(
                "SELECT table_name, table_type FROM information_schema.tables "
                "WHERE table_name IN ('sales_wide_raw', 'sales_long')"
            ).fetchall()
        )
    finally:
        conn.close()
    assert result.source_renamed_to == "sales_wide_raw"
    assert "sales_wide_raw" in names  # the renamed view
    assert "sales_long" in names  # the new long-form table
    assert "sales_wide" not in names
    assert kinds.get("sales_wide_raw") == "VIEW"
    assert kinds.get("sales_long") == "BASE TABLE"


# ---------------------------------------------------------------------------
# schema.yaml is kept in sync after apply
# ---------------------------------------------------------------------------


def _read_schema_yaml(project_dir: Path) -> dict:
    import yaml

    return yaml.safe_load((project_dir / "schema.yaml").read_text(encoding="utf-8"))


def test_update_schema_yaml_for_apply_keep_appends_target(tmp_path):
    """`keep` disposition: source entry stays, target appended."""
    (tmp_path / "schema.yaml").write_text(
        "tables:\n  - name: sales_wide\n    columns: [region, sales_2020]\n",
        encoding="utf-8",
    )
    rewrote = update_schema_yaml_for_apply(
        str(tmp_path),
        source_table="sales_wide",
        target_table="sales_long",
        disposition_mode="keep",
    )
    assert rewrote is True
    data = _read_schema_yaml(tmp_path)
    names = [t["name"] for t in data["tables"]]
    assert names == ["sales_wide", "sales_long"]
    # Source filter is left intact under `keep`.
    sales = next(t for t in data["tables"] if t["name"] == "sales_wide")
    assert sales.get("columns") == ["region", "sales_2020"]


def test_update_schema_yaml_for_apply_replace_clears_filter(tmp_path):
    """``replace`` disposition: entry name kept (long form took it over);
    column / excluded_columns filters cleared because the shape changed."""
    (tmp_path / "schema.yaml").write_text(
        "tables:\n"
        "  - name: sales_wide\n"
        "    columns: [region, sales_2020, sales_2021]\n"
        "  - name: customers\n",
        encoding="utf-8",
    )
    rewrote = update_schema_yaml_for_apply(
        str(tmp_path),
        source_table="sales_wide",
        target_table="sales_long",
        disposition_mode="replace",
    )
    assert rewrote is True
    data = _read_schema_yaml(tmp_path)
    names = [t["name"] for t in data["tables"]]
    assert names == ["sales_wide", "customers"]  # no `sales_long` entry
    sales = next(t for t in data["tables"] if t["name"] == "sales_wide")
    assert "columns" not in sales
    assert "excluded_columns" not in sales


def test_update_schema_yaml_for_apply_drop_removes_source(tmp_path):
    """``drop`` disposition: source entry removed, long form appended at
    its target name. Reflects that downstream references to the source
    name will break."""
    (tmp_path / "schema.yaml").write_text(
        "tables:\n  - name: sales_wide\n  - name: customers\n",
        encoding="utf-8",
    )
    rewrote = update_schema_yaml_for_apply(
        str(tmp_path),
        source_table="sales_wide",
        target_table="sales_long",
        disposition_mode="drop",
    )
    assert rewrote is True
    data = _read_schema_yaml(tmp_path)
    names = [t["name"] for t in data["tables"]]
    assert names == ["customers", "sales_long"]


def test_update_schema_yaml_for_apply_rename_renames_and_appends(tmp_path):
    """`rename` disposition: source entry renamed, target appended."""
    (tmp_path / "schema.yaml").write_text(
        "tables:\n  - name: sales_wide\n    columns: [region, sales_2020]\n",
        encoding="utf-8",
    )
    rewrote = update_schema_yaml_for_apply(
        str(tmp_path),
        source_table="sales_wide",
        target_table="sales_long",
        disposition_mode="rename",
        rename_to="sales_wide_raw",
    )
    assert rewrote is True
    data = _read_schema_yaml(tmp_path)
    names = [t["name"] for t in data["tables"]]
    assert names == ["sales_wide_raw", "sales_long"]
    # The source's column filter rides along with the renamed entry — the
    # raw wide table's columns haven't changed.
    raw = next(t for t in data["tables"] if t["name"] == "sales_wide_raw")
    assert raw.get("columns") == ["region", "sales_2020"]


def test_update_schema_yaml_for_apply_no_file_is_noop(tmp_path):
    """If the project has no schema.yaml, the helper does nothing."""
    rewrote = update_schema_yaml_for_apply(
        str(tmp_path),
        source_table="sales_wide",
        target_table="sales_long",
        disposition_mode="keep",
    )
    assert rewrote is False
    assert not (tmp_path / "schema.yaml").exists()


def test_update_schema_yaml_for_apply_creates_file_when_opt_in(tmp_path):
    """``create_if_absent=True`` materializes a fresh schema.yaml seeded
    with both the source and the new long-form table — used by the web
    Apply button so an explicit user action persists across restarts."""
    rewrote = update_schema_yaml_for_apply(
        str(tmp_path),
        source_table="sales_wide",
        target_table="sales_long",
        disposition_mode="keep",
        create_if_absent=True,
    )
    assert rewrote is True
    data = _read_schema_yaml(tmp_path)
    names = [t["name"] for t in data["tables"]]
    assert names == ["sales_wide", "sales_long"]


def test_update_schema_yaml_for_apply_keep_skips_duplicate_target(tmp_path):
    """If the target is already listed, don't add it twice."""
    (tmp_path / "schema.yaml").write_text(
        "tables:\n  - name: sales_wide\n  - name: sales_long\n",
        encoding="utf-8",
    )
    update_schema_yaml_for_apply(
        str(tmp_path),
        source_table="sales_wide",
        target_table="sales_long",
        disposition_mode="keep",
    )
    data = _read_schema_yaml(tmp_path)
    names = [t["name"] for t in data["tables"]]
    assert names == ["sales_wide", "sales_long"]


def test_cli_review_replace_source_updates_schema_yaml(tmp_path):
    """End-to-end: tidy review --replace-source rewrites schema.yaml so the
    long form (now under the source's old name) stays visible."""
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    project_dir = _project_with_db(tmp_path, db_path)
    (Path(project_dir) / "schema.yaml").write_text(
        "tables:\n  - name: sales_wide\n    columns: [region, sales_2020]\n",
        encoding="utf-8",
    )
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
            "--replace-source",
        ],
    )
    assert result.exit_code == 0, result.output
    data = _read_schema_yaml(Path(project_dir))
    names = [t["name"] for t in data["tables"]]
    assert "sales_wide" in names
    sales = next(t for t in data["tables"] if t["name"] == "sales_wide")
    # Old column filter is cleared so the new long-form columns are visible.
    assert "columns" not in sales


def test_cli_review_keep_source_appends_target_to_schema_yaml(tmp_path):
    """Default disposition: schema.yaml gains a new entry for the long form."""
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    project_dir = _project_with_db(tmp_path, db_path)
    (Path(project_dir) / "schema.yaml").write_text(
        "tables:\n  - name: sales_wide\n", encoding="utf-8"
    )
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
    data = _read_schema_yaml(Path(project_dir))
    names = [t["name"] for t in data["tables"]]
    assert names == ["sales_wide", "sales_long"]


def test_apply_proposal_rejects_view_mode_with_replace_source(tmp_path):
    """``--as view`` + ``replace`` is unsafe: dropping the source and
    renaming the view into its slot leaves the view recursively
    self-referencing (DuckDB raises "infinite recursion detected" on the
    next bind). The apply path must reject the combo before any DDL runs.
    """
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    plan = load_plan(_write_plan(tmp_path, _single_pivot_plan_dict()))
    conn = duckdb.connect(str(db_path))
    try:
        with pytest.raises(ValueError, match="'replace' requires --as table"):
            apply_proposal(
                conn,
                plan.proposals[0],
                mode="view",
                source_disposition=SourceDisposition(mode="replace"),
                dry_run=False,
            )
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    # Database is untouched: the wide source survives, no view leaked through.
    assert names == {"sales_wide"}


def test_apply_proposal_rejects_view_mode_with_drop_source(tmp_path):
    """``--as view`` + bare ``drop`` is unsafe: the view's body references
    the source by name, so dropping the source leaves the view dangling."""
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    plan = load_plan(_write_plan(tmp_path, _single_pivot_plan_dict()))
    conn = duckdb.connect(str(db_path))
    try:
        with pytest.raises(ValueError, match="'drop' requires --as table"):
            apply_proposal(
                conn,
                plan.proposals[0],
                mode="view",
                source_disposition=SourceDisposition(mode="drop"),
                dry_run=False,
            )
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert names == {"sales_wide"}


def test_apply_proposal_rejects_view_mode_with_rename_source(tmp_path):
    """``--as view`` + ``--rename-source`` would leave the view pointing at
    a missing object — reject the combo."""
    db_path = tmp_path / "wide.duckdb"
    _build_single_pivot_db(db_path)
    plan = load_plan(_write_plan(tmp_path, _single_pivot_plan_dict()))
    conn = duckdb.connect(str(db_path))
    try:
        with pytest.raises(ValueError, match="'rename' requires --as table"):
            apply_proposal(
                conn,
                plan.proposals[0],
                mode="view",
                source_disposition=SourceDisposition(mode="rename", new_name="sales_raw"),
                dry_run=False,
            )
    finally:
        conn.close()


def _normalize_cli_output(output: str) -> str:
    """Strip ANSI codes and collapse whitespace from CliRunner output.

    Click 8.3 + rich-click wraps usage errors in a Rich panel whose line
    breaks depend on the runner's terminal width. CI tends to wrap long
    error messages mid-phrase, so a literal ``in result.output`` check
    fails there even when it passes locally. Normalize before asserting.
    """
    plain = re.sub(r"\x1b\[[0-9;]*m", "", output)
    return re.sub(r"\s+", " ", plain)


def test_cli_review_view_mode_replace_source_is_a_usage_error(tmp_path):
    """``datasight tidy review --replace-source`` (default ``--as view``)
    must fail fast with a UsageError, not silently produce a recursive
    view."""
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
            # No --as flag → defaults to view, which is incompatible with replace.
            "--replace-source",
        ],
    )
    assert result.exit_code != 0
    assert "--replace-source requires '--as table'" in _normalize_cli_output(result.output)
    # No DDL ran.
    conn = duckdb.connect(str(db_path))
    try:
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()
    assert names == {"sales_wide"}


def test_cli_review_view_mode_drop_source_is_a_usage_error(tmp_path):
    """``--drop-source`` (post-rename) on view mode also rejects: a view
    referencing the source can't survive the source going away."""
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
        ],
    )
    assert result.exit_code != 0
    assert "--drop-source requires '--as table'" in _normalize_cli_output(result.output)


def test_cli_review_view_mode_rename_source_is_a_usage_error(tmp_path):
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
            "--rename-source",
            "sales_raw",
        ],
    )
    assert result.exit_code != 0
    assert "--rename-source requires '--as table'" in _normalize_cli_output(result.output)


def test_cli_review_replace_source_view_end_to_end(tmp_path):
    """The original user-reported flow: source is a CSV-backed view,
    ``--replace-source`` must succeed AND schema.yaml must keep the entry
    under the original name. (Previously this flag was ``--drop-source``.)"""
    db_path = tmp_path / "wide.duckdb"
    csv_path = tmp_path / "sales.csv"
    _build_single_pivot_view_db(db_path, csv_path)
    project_dir = _project_with_db(tmp_path, db_path)
    (Path(project_dir) / "schema.yaml").write_text(
        "tables:\n  - name: sales_wide\n", encoding="utf-8"
    )
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
            "--replace-source",
        ],
    )
    assert result.exit_code == 0, result.output
    conn = duckdb.connect(str(db_path))
    try:
        names = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        rows = conn.execute("SELECT COUNT(*) FROM sales_wide").fetchone()
    finally:
        conn.close()
    assert "sales_wide" in names
    assert rows is not None and rows[0] == 8
    data = _read_schema_yaml(Path(project_dir))
    names = [t["name"] for t in data["tables"]]
    assert names == ["sales_wide"]


# ---------------------------------------------------------------------------
# `datasight --verbose` toggles DEBUG logging across all commands
# ---------------------------------------------------------------------------


def test_cli_verbose_flag_enables_debug_logging(monkeypatch):
    """`datasight --verbose <cmd>` configures Loguru at DEBUG; the default
    is INFO. Verified by intercepting the call to ``configure_logging``
    that the group callback makes."""
    captured: list[str] = []

    def _capture(level: str = "INFO") -> None:
        captured.append(level)

    monkeypatch.setattr("datasight.cli.configure_logging", _capture)
    runner = CliRunner()
    # The group callback runs even when the subcommand fails to find work,
    # which is fine — we only care about the level it picks.
    runner.invoke(cli, ["--verbose", "doctor", "--help"])
    runner.invoke(cli, ["doctor", "--help"])
    assert captured == ["DEBUG", "INFO"]
