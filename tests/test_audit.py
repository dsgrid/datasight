"""Tests for the four audit CLI commands: integrity, distribution, validate, audit-report."""

import asyncio
import json
from pathlib import Path

import duckdb
from click.testing import CliRunner

from datasight.cli import cli
from datasight.distribution import build_distribution_overview
from datasight.integrity import build_integrity_overview
from datasight.runner import DuckDBRunner
from datasight.validation import build_validation_report


# ---------------------------------------------------------------------------
# Integrity
# ---------------------------------------------------------------------------


def test_integrity_table_format(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["integrity", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Referential Integrity" in result.output
    assert "Primary Keys" in result.output


def test_integrity_detects_pks(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["integrity", "--project-dir", project_dir, "--format", "json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["table_count"] >= 2
    pk_tables = {pk["table"] for pk in data["primary_keys"]}
    assert "products" in pk_tables
    assert "orders" in pk_tables
    # Both id columns should be unique
    for pk in data["primary_keys"]:
        assert pk["is_unique"] is True


def test_integrity_detects_fk_relationship(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["integrity", "--project-dir", project_dir, "--format", "json"])
    data = json.loads(result.output)
    # orders.product_id should be detected — no orphans expected
    assert data["orphan_foreign_keys"] == []
    # Check notes mention no orphans
    notes = " ".join(data["notes"])
    assert "orphan" in notes.lower() or "references resolve" in notes.lower()


def test_integrity_markdown(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["integrity", "--project-dir", project_dir, "--format", "markdown"]
    )
    assert result.exit_code == 0
    assert "# Referential Integrity" in result.output
    assert "## Primary Keys" in result.output


def test_integrity_single_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["integrity", "--project-dir", project_dir, "--table", "orders", "--format", "json"]
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["table_count"] == 1


def test_integrity_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "integrity.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "integrity",
            "--project-dir",
            project_dir,
            "--format",
            "json",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text())
    assert "primary_keys" in data


# ---------------------------------------------------------------------------
# Distribution
# ---------------------------------------------------------------------------


def test_distribution_table_format(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["distribution", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Distribution Profiling" in result.output
    assert "Distributions" in result.output


def test_distribution_json(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["distribution", "--project-dir", project_dir, "--format", "json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "distributions" in data
    assert len(data["distributions"]) > 0
    # Check that the expected keys exist on each distribution entry
    first = data["distributions"][0]
    for key in ("p5", "p50", "p95", "zero_rate", "negative_rate", "outlier_count"):
        assert key in first


def test_distribution_single_column(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "distribution",
            "--project-dir",
            project_dir,
            "--column",
            "orders.quantity",
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert len(data["distributions"]) == 1
    assert data["distributions"][0]["column"] == "quantity"


def test_distribution_markdown(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["distribution", "--project-dir", project_dir, "--format", "markdown"]
    )
    assert result.exit_code == 0
    assert "# Distribution Profiling" in result.output


def test_distribution_single_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["distribution", "--project-dir", project_dir, "--table", "products", "--format", "json"],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["table_count"] == 1
    for d in data["distributions"]:
        assert d["table"] == "products"


# ---------------------------------------------------------------------------
# Validate
# ---------------------------------------------------------------------------


def test_validate_no_config(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "No validation rules" in result.output
    # Should point users at the scaffold command, not ask them to hand-write YAML
    assert "--scaffold" in result.output


def test_validate_scaffold_creates_template(tmp_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--project-dir", str(tmp_path), "--scaffold"])
    assert result.exit_code == 0
    target = tmp_path / "validation.yaml"
    assert target.exists()
    content = target.read_text()
    # Template includes rule-type documentation and at least one example rule
    assert "required_columns" in content
    assert "- table:" in content


def test_validate_scaffold_refuses_to_clobber(tmp_path):
    target = tmp_path / "validation.yaml"
    target.write_text("existing: true\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--project-dir", str(tmp_path), "--scaffold"])
    assert result.exit_code != 0
    assert "already exists" in result.output
    # Existing content must not be overwritten
    assert target.read_text() == "existing: true\n"


def test_validate_scaffold_overwrite(tmp_path):
    target = tmp_path / "validation.yaml"
    target.write_text("existing: true\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        cli, ["validate", "--project-dir", str(tmp_path), "--scaffold", "--overwrite"]
    )
    assert result.exit_code == 0
    assert "required_columns" in target.read_text()


def test_validate_with_passing_rules(project_dir, tmp_path):
    config = tmp_path / "validation.yaml"
    config.write_text(
        """
- table: products
  rules:
    - type: required_columns
      columns: [id, name, category, price]
    - type: row_count
      min: 1
      max: 100
    - type: numeric_range
      column: price
      min: 0
""",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["validate", "--project-dir", project_dir, "--config", str(config), "--format", "json"],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["summary"]["fail"] == 0
    assert data["summary"]["pass"] == 3


def test_validate_with_failing_rules(project_dir, tmp_path):
    config = tmp_path / "validation.yaml"
    config.write_text(
        """
- table: products
  rules:
    - type: row_count
      min: 1000
""",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["validate", "--project-dir", project_dir, "--config", str(config), "--format", "json"],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["summary"]["fail"] == 1


def test_validate_allowed_values(project_dir, tmp_path):
    config = tmp_path / "validation.yaml"
    config.write_text(
        """
- table: products
  rules:
    - type: allowed_values
      column: category
      values: [electronics, tools, misc]
""",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["validate", "--project-dir", project_dir, "--config", str(config), "--format", "json"],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["summary"]["pass"] == 1


def test_validate_uniqueness(project_dir, tmp_path):
    config = tmp_path / "validation.yaml"
    config.write_text(
        """
- table: products
  rules:
    - type: uniqueness
      columns: [id]
""",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["validate", "--project-dir", project_dir, "--config", str(config), "--format", "json"],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["summary"]["pass"] == 1


def test_validate_markdown(project_dir, tmp_path):
    config = tmp_path / "validation.yaml"
    config.write_text(
        """
- table: products
  rules:
    - type: row_count
      min: 1
""",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--project-dir",
            project_dir,
            "--config",
            str(config),
            "--format",
            "markdown",
        ],
    )
    assert result.exit_code == 0
    assert "# Validation Report" in result.output


def test_validate_table_format(project_dir, tmp_path):
    config = tmp_path / "validation.yaml"
    config.write_text(
        """
- table: products
  rules:
    - type: row_count
      min: 1
""",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["validate", "--project-dir", project_dir, "--config", str(config)],
    )
    assert result.exit_code == 0
    assert "Validation Report" in result.output


# ---------------------------------------------------------------------------
# Validation rule handlers — unit tests, one pass + one fail per rule type
# ---------------------------------------------------------------------------


def _run_validation(schema_info, db_path, rules):
    """Helper: run build_validation_report against a DuckDB file."""

    async def go():
        runner = DuckDBRunner(database_path=str(db_path))
        try:
            return await build_validation_report(schema_info, runner.run_sql, rules)
        finally:
            runner.close()

    return asyncio.run(go())


def _events_db(tmp_path):
    """Build a small events table with known good and bad rows."""
    db_path = tmp_path / "events.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE events (
            id INTEGER,
            seq INTEGER,
            category VARCHAR,
            value DOUBLE,
            code VARCHAR,
            event_date DATE
        )
    """)
    # 10 rows: id duplicate (1,1), seq non-monotonic (5,3), category has 'BAD',
    # value has out-of-range -1 and 999, code has mismatched 'xx',
    # event_date is old, and value has 1 NULL (10% null rate).
    conn.execute("""
        INSERT INTO events VALUES
        (1, 1, 'A', 10.0,  'ab12', '2020-01-01'),
        (1, 2, 'A', 20.0,  'cd34', '2020-01-02'),
        (2, 3, 'B', -1.0,  'ef56', '2020-01-03'),
        (3, 5, 'B', 50.0,  'gh78', '2020-01-04'),
        (4, 3, 'C', 30.0,  'ij90', '2020-01-05'),
        (5, 6, 'C', 999.0, 'kl12', '2020-01-06'),
        (6, 7, 'BAD', 40.0,'mn34', '2020-01-07'),
        (7, 8, 'A', 25.0,  'op56', '2020-01-08'),
        (8, 9, 'B', 15.0,  'xx',   '2020-01-09'),
        (9, 10, 'C', NULL, 'qr78', '2020-01-10')
    """)
    conn.close()
    schema_info = [
        {
            "name": "events",
            "row_count": 10,
            "columns": [
                {"name": "id", "dtype": "INTEGER"},
                {"name": "seq", "dtype": "INTEGER"},
                {"name": "category", "dtype": "VARCHAR"},
                {"name": "value", "dtype": "DOUBLE"},
                {"name": "code", "dtype": "VARCHAR"},
                {"name": "event_date", "dtype": "DATE"},
            ],
        }
    ]
    return schema_info, db_path


def _single_result(report, rule_type):
    matches = [r for r in report["results"] if r["rule"] == rule_type]
    assert len(matches) == 1, f"expected one {rule_type} result, got {matches}"
    return matches[0]


def test_validate_required_columns_pass_and_fail(tmp_path):
    schema, db = _events_db(tmp_path)

    pass_report = _run_validation(
        schema,
        db,
        [{"table": "events", "rules": [{"type": "required_columns", "columns": ["id", "value"]}]}],
    )
    assert _single_result(pass_report, "required_columns")["status"] == "pass"

    fail_report = _run_validation(
        schema,
        db,
        [
            {
                "table": "events",
                "rules": [{"type": "required_columns", "columns": ["id", "missing_col"]}],
            }
        ],
    )
    r = _single_result(fail_report, "required_columns")
    assert r["status"] == "fail"
    assert "missing_col" in r["detail"]


def test_validate_max_null_rate_pass_and_fail(tmp_path):
    schema, db = _events_db(tmp_path)

    # value has 1 NULL out of 10 = 10% null rate
    pass_report = _run_validation(
        schema,
        db,
        [
            {
                "table": "events",
                "rules": [{"type": "max_null_rate", "column": "value", "threshold": 0.5}],
            }
        ],
    )
    assert _single_result(pass_report, "max_null_rate")["status"] == "pass"

    fail_report = _run_validation(
        schema,
        db,
        [
            {
                "table": "events",
                "rules": [{"type": "max_null_rate", "column": "value", "threshold": 0.05}],
            }
        ],
    )
    r = _single_result(fail_report, "max_null_rate")
    assert r["status"] == "fail"
    assert "exceeds" in r["detail"].lower()


def test_validate_numeric_range_pass_and_fail(tmp_path):
    schema, db = _events_db(tmp_path)

    pass_report = _run_validation(
        schema,
        db,
        [
            {
                "table": "events",
                "rules": [{"type": "numeric_range", "column": "value", "min": -5, "max": 1000}],
            }
        ],
    )
    assert _single_result(pass_report, "numeric_range")["status"] == "pass"

    fail_report = _run_validation(
        schema,
        db,
        [
            {
                "table": "events",
                "rules": [{"type": "numeric_range", "column": "value", "min": 0, "max": 100}],
            }
        ],
    )
    r = _single_result(fail_report, "numeric_range")
    assert r["status"] == "fail"
    assert "min" in r["detail"] and "max" in r["detail"]


def test_validate_allowed_values_pass_and_fail(tmp_path):
    schema, db = _events_db(tmp_path)

    pass_report = _run_validation(
        schema,
        db,
        [
            {
                "table": "events",
                "rules": [
                    {
                        "type": "allowed_values",
                        "column": "category",
                        "values": ["A", "B", "C", "BAD"],
                    }
                ],
            }
        ],
    )
    assert _single_result(pass_report, "allowed_values")["status"] == "pass"

    fail_report = _run_validation(
        schema,
        db,
        [
            {
                "table": "events",
                "rules": [
                    {"type": "allowed_values", "column": "category", "values": ["A", "B", "C"]}
                ],
            }
        ],
    )
    r = _single_result(fail_report, "allowed_values")
    assert r["status"] == "fail"
    assert "BAD" in r["detail"]


def test_validate_regex_pass_and_fail(tmp_path):
    schema, db = _events_db(tmp_path)

    # All codes are non-empty strings
    pass_report = _run_validation(
        schema,
        db,
        [{"table": "events", "rules": [{"type": "regex", "column": "code", "pattern": ".+"}]}],
    )
    assert _single_result(pass_report, "regex")["status"] == "pass"

    # Stricter pattern: two letters + two digits — 'xx' violates
    fail_report = _run_validation(
        schema,
        db,
        [
            {
                "table": "events",
                "rules": [{"type": "regex", "column": "code", "pattern": "^[a-z]{2}[0-9]{2}$"}],
            }
        ],
    )
    r = _single_result(fail_report, "regex")
    assert r["status"] == "fail"
    assert r["value"] == 1


def test_validate_uniqueness_pass_and_fail(tmp_path):
    schema, db = _events_db(tmp_path)

    # (id, seq) is unique
    pass_report = _run_validation(
        schema,
        db,
        [{"table": "events", "rules": [{"type": "uniqueness", "columns": ["id", "seq"]}]}],
    )
    assert _single_result(pass_report, "uniqueness")["status"] == "pass"

    # id alone has a duplicate (1)
    fail_report = _run_validation(
        schema,
        db,
        [{"table": "events", "rules": [{"type": "uniqueness", "columns": ["id"]}]}],
    )
    r = _single_result(fail_report, "uniqueness")
    assert r["status"] == "fail"


def test_validate_monotonic_pass_and_fail(tmp_path):
    schema, db = _events_db(tmp_path)

    # id when ordered by id is non_decreasing (allows equal) — 1,1,2,3,...
    pass_report = _run_validation(
        schema,
        db,
        [
            {
                "table": "events",
                "rules": [{"type": "monotonic", "column": "id", "direction": "non_decreasing"}],
            }
        ],
    )
    assert _single_result(pass_report, "monotonic")["status"] == "pass"

    # id requires strictly increasing — the 1,1 duplicate violates
    fail_report = _run_validation(
        schema,
        db,
        [
            {
                "table": "events",
                "rules": [{"type": "monotonic", "column": "id", "direction": "increasing"}],
            }
        ],
    )
    r = _single_result(fail_report, "monotonic")
    assert r["status"] == "fail"


def test_validate_row_count_pass_and_fail(tmp_path):
    schema, db = _events_db(tmp_path)

    pass_report = _run_validation(
        schema,
        db,
        [{"table": "events", "rules": [{"type": "row_count", "min": 5, "max": 20}]}],
    )
    assert _single_result(pass_report, "row_count")["status"] == "pass"

    fail_report = _run_validation(
        schema,
        db,
        [{"table": "events", "rules": [{"type": "row_count", "min": 100}]}],
    )
    assert _single_result(fail_report, "row_count")["status"] == "fail"


def test_validate_freshness_pass_and_fail(tmp_path):
    schema, db = _events_db(tmp_path)

    # event dates are in 2020 — stale by any reasonable SLA
    fail_report = _run_validation(
        schema,
        db,
        [
            {
                "table": "events",
                "rules": [{"type": "freshness", "column": "event_date", "max_age_days": 90}],
            }
        ],
    )
    assert _single_result(fail_report, "freshness")["status"] == "fail"

    # ...but a very generous SLA covers them
    pass_report = _run_validation(
        schema,
        db,
        [
            {
                "table": "events",
                "rules": [{"type": "freshness", "column": "event_date", "max_age_days": 100000}],
            }
        ],
    )
    assert _single_result(pass_report, "freshness")["status"] == "pass"


def test_validate_unknown_rule_type_warns(tmp_path):
    schema, db = _events_db(tmp_path)
    report = _run_validation(
        schema,
        db,
        [{"table": "events", "rules": [{"type": "no_such_rule"}]}],
    )
    assert report["summary"]["warn"] >= 1


def test_validate_missing_table_fails(tmp_path):
    schema, db = _events_db(tmp_path)
    report = _run_validation(
        schema,
        db,
        [{"table": "nonexistent", "rules": [{"type": "row_count", "min": 1}]}],
    )
    assert any(r["status"] == "fail" and r["rule"] == "table_exists" for r in report["results"])


# ---------------------------------------------------------------------------
# Integrity — unit tests against deliberately broken fixtures
# ---------------------------------------------------------------------------


def _run_integrity(schema_info, db_path, declared_joins=None):
    async def go():
        runner = DuckDBRunner(database_path=str(db_path))
        try:
            return await build_integrity_overview(schema_info, runner.run_sql, declared_joins)
        finally:
            runner.close()

    return asyncio.run(go())


def _integrity_db(tmp_path):
    """Build a DB with a valid parent, an orphan FK, a duplicate PK, and a join explosion."""
    db_path = tmp_path / "integrity.duckdb"
    conn = duckdb.connect(str(db_path))
    # Parent with unique id
    conn.execute("CREATE TABLE parents (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO parents VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    # Child with orphan parent_id=99 and a duplicate child id
    conn.execute("CREATE TABLE children (id INTEGER, parent_id INTEGER, value INTEGER)")
    conn.execute("""
        INSERT INTO children VALUES
        (1, 1, 10),
        (1, 1, 11),
        (2, 2, 20),
        (3, 99, 30)
    """)
    conn.close()
    schema_info = [
        {
            "name": "parents",
            "row_count": 3,
            "columns": [
                {"name": "id", "dtype": "INTEGER"},
                {"name": "name", "dtype": "VARCHAR"},
            ],
        },
        {
            "name": "children",
            "row_count": 4,
            "columns": [
                {"name": "id", "dtype": "INTEGER"},
                {"name": "parent_id", "dtype": "INTEGER"},
                {"name": "value", "dtype": "INTEGER"},
            ],
        },
    ]
    return schema_info, db_path


def test_integrity_detects_duplicate_primary_key(tmp_path):
    schema, db = _integrity_db(tmp_path)
    report = _run_integrity(schema, db)
    # children.id has a duplicate (1)
    dupes = [d for d in report["duplicate_keys"] if d["table"] == "children"]
    assert dupes, f"expected duplicate PK on children, got {report['duplicate_keys']}"
    assert dupes[0]["column"] == "id"


def test_integrity_detects_orphan_foreign_keys(tmp_path):
    schema, db = _integrity_db(tmp_path)
    report = _run_integrity(schema, db)
    orphans = report["orphan_foreign_keys"]
    assert orphans, "expected children.parent_id=99 to be flagged as an orphan"
    orphan = orphans[0]
    assert orphan["child_table"] == "children"
    assert orphan["child_column"] == "parent_id"
    assert orphan["parent_table"] == "parents"
    assert orphan["orphan_count"] >= 1


def test_integrity_detects_join_explosion(tmp_path):
    """Join explosion happens when the child FK matches multiple rows in the parent."""
    db_path = tmp_path / "explosion.duckdb"
    conn = duckdb.connect(str(db_path))
    # Parent has a duplicated "primary" key — multi-row match
    conn.execute("CREATE TABLE parents (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO parents VALUES (1, 'a1'), (1, 'a2'), (2, 'b')")
    conn.execute("CREATE TABLE children (id INTEGER, parent_id INTEGER)")
    conn.execute("INSERT INTO children VALUES (1, 1), (2, 2), (3, 2)")
    conn.close()
    schema_info = [
        {
            "name": "parents",
            "row_count": 3,
            "columns": [
                {"name": "id", "dtype": "INTEGER"},
                {"name": "name", "dtype": "VARCHAR"},
            ],
        },
        {
            "name": "children",
            "row_count": 3,
            "columns": [
                {"name": "id", "dtype": "INTEGER"},
                {"name": "parent_id", "dtype": "INTEGER"},
            ],
        },
    ]
    report = _run_integrity(schema_info, db_path)
    assert report["join_explosions"], f"expected join explosion, got {report}"
    boom = report["join_explosions"][0]
    assert boom["explosion_factor"] > 1.0


def test_integrity_honors_declared_joins(tmp_path):
    """Columns that don't follow the `_id` naming convention still get checked when declared."""
    db_path = tmp_path / "declared.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE codes (code VARCHAR, label VARCHAR)")
    conn.execute("INSERT INTO codes VALUES ('A', 'alpha'), ('B', 'beta')")
    conn.execute("CREATE TABLE events (id INTEGER, code VARCHAR)")
    # 'Z' is not in codes.code -> orphan
    conn.execute("INSERT INTO events VALUES (1, 'A'), (2, 'Z')")
    conn.close()
    schema_info = [
        {
            "name": "codes",
            "row_count": 2,
            "columns": [
                {"name": "code", "dtype": "VARCHAR"},
                {"name": "label", "dtype": "VARCHAR"},
            ],
        },
        {
            "name": "events",
            "row_count": 2,
            "columns": [
                {"name": "id", "dtype": "INTEGER"},
                {"name": "code", "dtype": "VARCHAR"},
            ],
        },
    ]
    declared = [
        {
            "child_table": "events",
            "child_column": "code",
            "parent_table": "codes",
            "parent_column": "code",
        }
    ]
    report = _run_integrity(schema_info, db_path, declared_joins=declared)
    orphans = [
        o
        for o in report["orphan_foreign_keys"]
        if o["child_column"] == "code" and o["parent_table"] == "codes"
    ]
    assert orphans, f"declared join should surface orphan, got {report}"


# ---------------------------------------------------------------------------
# Distribution — unit tests for energy flags and spikes
# ---------------------------------------------------------------------------


def _run_distribution(schema_info, db_path, overrides=None, target_column=None):
    async def go():
        runner = DuckDBRunner(database_path=str(db_path))
        try:
            return await build_distribution_overview(
                schema_info, runner.run_sql, overrides=overrides, target_column=target_column
            )
        finally:
            runner.close()

    return asyncio.run(go())


def test_distribution_flags_negative_generation(tmp_path):
    db_path = tmp_path / "neg_gen.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE plant (net_generation_mwh DOUBLE)")
    conn.execute(
        "INSERT INTO plant VALUES (100), (120), (80), (-5), (-10), (90), (110), (95), (105), (100)"
    )
    conn.close()
    schema_info = [
        {
            "name": "plant",
            "row_count": 10,
            "columns": [{"name": "net_generation_mwh", "dtype": "DOUBLE"}],
        }
    ]
    report = _run_distribution(schema_info, db_path)
    flags = [f for f in report["energy_flags"] if f["flag"] == "negative_generation"]
    assert flags, f"expected negative_generation flag, got {report['energy_flags']}"


def test_distribution_flags_capacity_factor_over_100(tmp_path):
    db_path = tmp_path / "cf.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE plant (capacity_factor_pct DOUBLE)")
    # Values are clearly percentages (>100 indicates nonsense)
    conn.execute(
        "INSERT INTO plant VALUES (50), (60), (70), (80), (90), (95), (100), (105), (110), (120)"
    )
    conn.close()
    schema_info = [
        {
            "name": "plant",
            "row_count": 10,
            "columns": [{"name": "capacity_factor_pct", "dtype": "DOUBLE"}],
        }
    ]
    report = _run_distribution(schema_info, db_path)
    flags = [f for f in report["energy_flags"] if f["flag"].startswith("capacity_factor_over")]
    assert flags, f"expected capacity_factor flag, got {report['energy_flags']}"


def test_distribution_flags_zero_values_in_price(tmp_path):
    db_path = tmp_path / "price.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE market (lmp_usd_per_mwh DOUBLE)")
    conn.execute(
        "INSERT INTO market VALUES (25), (30), (0), (0), (35), (40), (45), (0), (50), (55)"
    )
    conn.close()
    schema_info = [
        {
            "name": "market",
            "row_count": 10,
            "columns": [{"name": "lmp_usd_per_mwh", "dtype": "DOUBLE"}],
        }
    ]
    report = _run_distribution(schema_info, db_path)
    flags = [f for f in report["energy_flags"] if f["flag"] == "zero_values_in_rate"]
    assert flags, f"expected zero_values_in_rate flag, got {report['energy_flags']}"


def test_distribution_suppresses_energy_note_for_non_energy_datasets(tmp_path):
    """If no column has energy semantics, the 'no energy anomalies' note must NOT appear."""
    db_path = tmp_path / "generic.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE widgets (id INTEGER, weight DOUBLE, width DOUBLE)")
    conn.execute(
        "INSERT INTO widgets VALUES "
        "(1, 1.0, 5.0), (2, 2.0, 6.0), (3, 3.0, 7.0), (4, 4.0, 8.0), (5, 5.0, 9.0)"
    )
    conn.close()
    schema_info = [
        {
            "name": "widgets",
            "row_count": 5,
            "columns": [
                {"name": "id", "dtype": "INTEGER"},
                {"name": "weight", "dtype": "DOUBLE"},
                {"name": "width", "dtype": "DOUBLE"},
            ],
        }
    ]
    report = _run_distribution(schema_info, db_path)
    joined = " ".join(report["notes"])
    assert "energy-specific anomalies" not in joined


def test_distribution_counts_iqr_outliers(tmp_path):
    db_path = tmp_path / "outliers.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE t (val DOUBLE)")
    # 9 values around 5, one extreme outlier at 1000
    conn.execute("INSERT INTO t VALUES (4), (5), (5), (5), (6), (5), (5), (5), (4), (1000)")
    conn.close()
    schema_info = [{"name": "t", "row_count": 10, "columns": [{"name": "val", "dtype": "DOUBLE"}]}]
    report = _run_distribution(schema_info, db_path)
    val = next(d for d in report["distributions"] if d["column"] == "val")
    assert val["outlier_count"] >= 1


def test_distribution_detects_temporal_spike(tmp_path):
    db_path = tmp_path / "spike.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE ts (ts_date DATE, val DOUBLE)")
    # 10 flat months, then an enormous spike month — z-score must exceed 3σ
    rows = []
    for month in range(1, 11):
        for day in (1, 10, 20):
            rows.append(f"('2024-{month:02d}-{day:02d}', 10)")
    # Spike month with 3 huge readings
    for day in (1, 10, 20):
        rows.append(f"('2025-01-{day:02d}', 10000)")
    conn.execute(f"INSERT INTO ts VALUES {', '.join(rows)}")
    conn.close()
    schema_info = [
        {
            "name": "ts",
            "row_count": 33,
            "columns": [
                {"name": "ts_date", "dtype": "DATE"},
                {"name": "val", "dtype": "DOUBLE"},
            ],
        }
    ]
    report = _run_distribution(schema_info, db_path)
    assert report["spikes"], f"expected at least one temporal spike, got {report}"


# ---------------------------------------------------------------------------
# Audit report — validation integration
# ---------------------------------------------------------------------------


def test_audit_report_includes_validation_when_configured(project_dir, tmp_path):
    """When validation.yaml exists, audit-report runs the rules and includes results."""
    (Path(project_dir) / "validation.yaml").write_text(
        """
- table: products
  rules:
    - type: row_count
      min: 1000
""",
        encoding="utf-8",
    )
    output_path = tmp_path / "report.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "audit-report",
            "--project-dir",
            project_dir,
            "--output",
            str(output_path),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text())
    assert data["validation"] is not None
    assert data["validation"]["summary"]["fail"] >= 1


def test_distribution_handles_all_null_column(tmp_path):
    """All-NULL numeric columns must not crash with NaN-to-int errors."""
    db_path = tmp_path / "nulls.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE t (id INTEGER, val DOUBLE)")
    conn.execute("INSERT INTO t VALUES (1, NULL), (2, NULL), (3, NULL)")
    conn.close()

    schema_info = [
        {
            "name": "t",
            "columns": [
                {"name": "id", "dtype": "INTEGER"},
                {"name": "val", "dtype": "DOUBLE"},
            ],
        }
    ]

    async def run():
        runner = DuckDBRunner(database_path=str(db_path))
        try:
            return await build_distribution_overview(schema_info, runner.run_sql)
        finally:
            runner.close()

    result = asyncio.run(run())
    dist = next(d for d in result["distributions"] if d["column"] == "val")
    # Percentiles should be None (not NaN) — and nothing should have crashed
    for key in ("p1", "p5", "p50", "p95", "p99", "q1", "q3", "mean", "stddev"):
        assert dist[key] is None, f"{key} should be None for all-NULL column, got {dist[key]!r}"
    assert dist["outlier_count"] == 0


# ---------------------------------------------------------------------------
# Audit report
# ---------------------------------------------------------------------------


def test_audit_report_html(project_dir, tmp_path):
    output_path = tmp_path / "report.html"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["audit-report", "--project-dir", project_dir, "--output", str(output_path)],
    )
    assert result.exit_code == 0
    html = output_path.read_text()
    assert "<html" in html
    assert "datasight Audit Report" in html
    assert "Data Quality" in html
    assert "Referential Integrity" in html
    assert "Distribution Profiling" in html


def test_audit_report_markdown(project_dir, tmp_path):
    output_path = tmp_path / "report.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["audit-report", "--project-dir", project_dir, "--output", str(output_path)],
    )
    assert result.exit_code == 0
    md = output_path.read_text()
    assert "# datasight Audit Report" in md
    assert "## Data Quality" in md
    assert "## Referential Integrity" in md
    assert "## Distribution Profiling" in md


def test_audit_report_json(project_dir, tmp_path):
    output_path = tmp_path / "report.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "audit-report",
            "--project-dir",
            project_dir,
            "--output",
            str(output_path),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text())
    assert "generated_at" in data
    assert "dataset_overview" in data
    assert "quality" in data
    assert "integrity" in data
    assert "distribution" in data


def test_audit_report_includes_project_name(project_dir, tmp_path):
    output_path = tmp_path / "report.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "audit-report",
            "--project-dir",
            project_dir,
            "--output",
            str(output_path),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text())
    # project_name defaults to the basename of project_dir
    assert data["project_name"] == Path(project_dir).name


def test_audit_report_html_title_includes_project_name(project_dir, tmp_path):
    output_path = tmp_path / "report.html"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["audit-report", "--project-dir", project_dir, "--output", str(output_path)],
    )
    assert result.exit_code == 0
    html = output_path.read_text()
    name = Path(project_dir).name
    assert name in html


def test_audit_report_markdown_title_includes_project_name(project_dir, tmp_path):
    output_path = tmp_path / "report.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["audit-report", "--project-dir", project_dir, "--output", str(output_path)],
    )
    assert result.exit_code == 0
    md = output_path.read_text()
    name = Path(project_dir).name
    # First line is the title and should include the project name
    assert md.splitlines()[0] == f"# datasight Audit Report — {name}"


def test_audit_report_default_output_is_report_html(project_dir, tmp_path, monkeypatch):
    # Run with no -o; it should default to ./report.html in the cwd
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["audit-report", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert (tmp_path / "report.html").exists()


def test_audit_report_single_table(project_dir, tmp_path):
    output_path = tmp_path / "report.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "audit-report",
            "--project-dir",
            project_dir,
            "--table",
            "products",
            "--output",
            str(output_path),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text())
    assert data["dataset_overview"]["table_count"] == 1
