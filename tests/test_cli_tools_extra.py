"""Additional CLI coverage tests targeting uncovered commands and branches."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from datasight.cli import cli

# Env vars that leak across test isolation via load_dotenv (override=False).
_DATASIGHT_ENV_VARS = (
    "DB_MODE",
    "DB_PATH",
    "LLM_PROVIDER",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_MODEL",
    "OLLAMA_MODEL",
    "OLLAMA_BASE_URL",
    "GITHUB_TOKEN",
    "GITHUB_MODEL",
)


@pytest.fixture
def clean_env(monkeypatch):
    """Scrub datasight env vars so per-test .env files take effect."""
    for key in _DATASIGHT_ENV_VARS:
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


# ---------------------------------------------------------------------------
# init command
# ---------------------------------------------------------------------------


def test_init_creates_template_files(tmp_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["init", str(tmp_path)])
    assert result.exit_code == 0
    assert "Project initialized" in result.output
    assert (tmp_path / ".env").exists()
    assert (tmp_path / "schema_description.md").exists()
    assert (tmp_path / "queries.yaml").exists()


def test_init_skips_existing_files_without_overwrite(tmp_path):
    (tmp_path / ".env").write_text("EXISTING=1\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(cli, ["init", str(tmp_path)])
    assert result.exit_code == 0
    assert "Skipped" in result.output
    # Existing content preserved
    assert (tmp_path / ".env").read_text(encoding="utf-8") == "EXISTING=1\n"


def test_init_overwrites_when_flag_set(tmp_path):
    (tmp_path / ".env").write_text("EXISTING=1\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(cli, ["init", str(tmp_path), "--overwrite"])
    assert result.exit_code == 0
    # Template content should have replaced the placeholder
    assert (tmp_path / ".env").read_text(encoding="utf-8") != "EXISTING=1\n"


# ---------------------------------------------------------------------------
# integrity command
# ---------------------------------------------------------------------------


def test_integrity_table_output(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["integrity", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Referential Integrity" in result.output


def test_integrity_json_output(project_dir, tmp_path):
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
    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert "table_count" in data


def test_integrity_markdown_output(project_dir, tmp_path):
    output_path = tmp_path / "integrity.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "integrity",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    assert "# Referential Integrity" in output_path.read_text(encoding="utf-8")


def test_integrity_scope_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["integrity", "--project-dir", project_dir, "--table", "orders"])
    assert result.exit_code == 0


def test_integrity_missing_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["integrity", "--project-dir", project_dir, "--table", "nosuchtable"]
    )
    assert result.exit_code != 0
    assert "Table not found" in result.output


def test_integrity_missing_db_file(tmp_path, clean_env):
    (tmp_path / ".env").write_text(
        "LLM_PROVIDER=ollama\nDB_MODE=duckdb\nDB_PATH=nope.duckdb\n", encoding="utf-8"
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["integrity", "--project-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "Database file not found" in result.output


# ---------------------------------------------------------------------------
# distribution command
# ---------------------------------------------------------------------------


def test_distribution_table_output(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["distribution", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Distribution Profiling" in result.output


def test_distribution_json_output(project_dir, tmp_path):
    output_path = tmp_path / "dist.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "distribution",
            "--project-dir",
            project_dir,
            "--format",
            "json",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert "table_count" in data


def test_distribution_markdown_output(project_dir, tmp_path):
    output_path = tmp_path / "dist.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "distribution",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    assert "# Distribution Profiling" in output_path.read_text(encoding="utf-8")


def test_distribution_table_scope(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["distribution", "--project-dir", project_dir, "--table", "orders"]
    )
    assert result.exit_code == 0


def test_distribution_column_scope(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["distribution", "--project-dir", project_dir, "--column", "orders.quantity"],
    )
    assert result.exit_code == 0


def test_distribution_table_and_column_mutual_exclusion(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "distribution",
            "--project-dir",
            project_dir,
            "--table",
            "orders",
            "--column",
            "orders.quantity",
        ],
    )
    assert result.exit_code != 0
    assert "use either --table or --column" in result.output


def test_distribution_missing_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["distribution", "--project-dir", project_dir, "--table", "nosuchtable"]
    )
    assert result.exit_code != 0
    assert "Table not found" in result.output


def test_distribution_missing_db_file(tmp_path, clean_env):
    (tmp_path / ".env").write_text(
        "LLM_PROVIDER=ollama\nDB_MODE=duckdb\nDB_PATH=nope.duckdb\n", encoding="utf-8"
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["distribution", "--project-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "Database file not found" in result.output


# ---------------------------------------------------------------------------
# validate command
# ---------------------------------------------------------------------------


def test_validate_scaffold_creates_file(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--project-dir", project_dir, "--scaffold"])
    assert result.exit_code == 0
    target = Path(project_dir) / "validation.yaml"
    assert target.exists()
    assert "Wrote" in result.output


def test_validate_scaffold_refuses_to_overwrite(project_dir):
    target = Path(project_dir) / "validation.yaml"
    target.write_text("existing: true\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--project-dir", project_dir, "--scaffold"])
    assert result.exit_code != 0
    assert "already exists" in result.output


def test_validate_scaffold_overwrite(project_dir):
    target = Path(project_dir) / "validation.yaml"
    target.write_text("existing: true\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        cli, ["validate", "--project-dir", project_dir, "--scaffold", "--overwrite"]
    )
    assert result.exit_code == 0
    assert target.read_text(encoding="utf-8") != "existing: true\n"


def test_validate_no_rules_configured(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "No validation rules configured" in result.output


def test_validate_with_rules_runs(project_dir):
    rules = Path(project_dir) / "validation.yaml"
    rules.write_text(
        ("- table: orders\n  rule: not_null\n  column: quantity\n"),
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Validation Report" in result.output


def test_validate_json_output(project_dir, tmp_path):
    rules = Path(project_dir) / "validation.yaml"
    rules.write_text("- table: orders\n  rule: not_null\n  column: quantity\n", encoding="utf-8")
    output_path = tmp_path / "validate.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--project-dir",
            project_dir,
            "--format",
            "json",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert "results" in data


def test_validate_markdown_output(project_dir, tmp_path):
    rules = Path(project_dir) / "validation.yaml"
    rules.write_text("- table: orders\n  rule: not_null\n  column: quantity\n", encoding="utf-8")
    output_path = tmp_path / "validate.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    assert "Validation" in output_path.read_text(encoding="utf-8")


def test_validate_filter_no_rules_for_table(project_dir):
    rules = Path(project_dir) / "validation.yaml"
    rules.write_text("- table: orders\n  rule: not_null\n  column: quantity\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--project-dir", project_dir, "--table", "products"])
    assert result.exit_code == 0
    assert "No validation rules found for table" in result.output


def test_validate_missing_db_file(tmp_path, clean_env):
    (tmp_path / ".env").write_text(
        "LLM_PROVIDER=ollama\nDB_MODE=duckdb\nDB_PATH=nope.duckdb\n", encoding="utf-8"
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--project-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "Database file not found" in result.output


# ---------------------------------------------------------------------------
# audit-report command
# ---------------------------------------------------------------------------


def test_audit_report_html_default(project_dir, tmp_path):
    output_path = tmp_path / "audit.html"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "audit-report",
            "--project-dir",
            project_dir,
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    assert output_path.exists()
    assert "<html" in output_path.read_text(encoding="utf-8").lower()


def test_audit_report_json(project_dir, tmp_path):
    output_path = tmp_path / "audit.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "audit-report",
            "--project-dir",
            project_dir,
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert isinstance(data, dict)


def test_audit_report_markdown(project_dir, tmp_path):
    output_path = tmp_path / "audit.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "audit-report",
            "--project-dir",
            project_dir,
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    assert len(output_path.read_text(encoding="utf-8")) > 0


def test_audit_report_scope_missing_table(project_dir, tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "audit-report",
            "--project-dir",
            project_dir,
            "--table",
            "nosuchtable",
            "--output",
            str(tmp_path / "a.html"),
        ],
    )
    assert result.exit_code != 0
    assert "Table not found" in result.output


def test_audit_report_missing_db(tmp_path, clean_env):
    (tmp_path / ".env").write_text(
        "LLM_PROVIDER=ollama\nDB_MODE=duckdb\nDB_PATH=nope.duckdb\n", encoding="utf-8"
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["audit-report", "--project-dir", str(tmp_path), "--output", str(tmp_path / "a.html")]
    )
    assert result.exit_code != 0
    assert "Database file not found" in result.output


# ---------------------------------------------------------------------------
# export command
# ---------------------------------------------------------------------------


def test_export_list_no_conversations(tmp_path):
    (tmp_path / ".env").write_text("LLM_PROVIDER=ollama\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(cli, ["export", "--project-dir", str(tmp_path), "--list-sessions"])
    assert result.exit_code == 0
    assert "No conversations" in result.output


def test_export_requires_session_id_without_list_sessions(tmp_path):
    (tmp_path / ".env").write_text("LLM_PROVIDER=ollama\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(cli, ["export", "--project-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "provide a SESSION_ID" in result.output


def test_export_session_not_found(tmp_path):
    (tmp_path / ".env").write_text("LLM_PROVIDER=ollama\n", encoding="utf-8")
    # Create the conv dir but no matching session
    (tmp_path / ".datasight" / "conversations").mkdir(parents=True)
    runner = CliRunner()
    result = runner.invoke(cli, ["export", "nonexistent", "--project-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "Session not found" in result.output


def test_export_session_success(tmp_path):
    (tmp_path / ".env").write_text("LLM_PROVIDER=ollama\n", encoding="utf-8")
    conv_dir = tmp_path / ".datasight" / "conversations"
    conv_dir.mkdir(parents=True)
    session_file = conv_dir / "sess1.json"
    session_data = {
        "title": "Test Session",
        "events": [
            {"event": "user_message", "data": {"message": "hello"}},
            {"event": "assistant_message", "data": {"message": "hi"}},
        ],
    }
    session_file.write_text(json.dumps(session_data), encoding="utf-8")
    output_path = tmp_path / "out.html"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "export",
            "sess1",
            "--project-dir",
            str(tmp_path),
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert output_path.exists()


def test_export_list_sessions_shows_table(tmp_path):
    (tmp_path / ".env").write_text("LLM_PROVIDER=ollama\n", encoding="utf-8")
    conv_dir = tmp_path / ".datasight" / "conversations"
    conv_dir.mkdir(parents=True)
    (conv_dir / "sess1.json").write_text(
        json.dumps(
            {
                "title": "Session 1",
                "events": [{"event": "user_message", "data": {"message": "hi"}}],
            }
        ),
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["export", "list", "--project-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "sess1" in result.output


def test_export_invalid_exclude(tmp_path):
    (tmp_path / ".env").write_text("LLM_PROVIDER=ollama\n", encoding="utf-8")
    conv_dir = tmp_path / ".datasight" / "conversations"
    conv_dir.mkdir(parents=True)
    (conv_dir / "sess1.json").write_text(
        json.dumps({"title": "T", "events": [{"event": "user_message", "data": {"m": "x"}}]}),
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["export", "sess1", "--project-dir", str(tmp_path), "--exclude", "not-a-number"],
    )
    assert result.exit_code != 0
    assert "comma-separated integers" in result.output


def test_export_empty_events(tmp_path):
    (tmp_path / ".env").write_text("LLM_PROVIDER=ollama\n", encoding="utf-8")
    conv_dir = tmp_path / ".datasight" / "conversations"
    conv_dir.mkdir(parents=True)
    (conv_dir / "empty.json").write_text(json.dumps({"events": []}), encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(cli, ["export", "empty", "--project-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "no events" in result.output.lower()


# ---------------------------------------------------------------------------
# log command
# ---------------------------------------------------------------------------


def test_log_missing_file(tmp_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["log", "--project-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "No query log" in result.output


def _write_query_log(tmp_path, entries):
    log_path = tmp_path / ".datasight" / "query_log.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("\n".join(json.dumps(e) for e in entries) + "\n", encoding="utf-8")
    return log_path


def test_log_table_output(tmp_path):
    _write_query_log(
        tmp_path,
        [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "tool": "run_sql",
                "sql": "SELECT 1",
                "execution_time_ms": 12.3,
                "row_count": 1,
                "user_question": "test",
            },
            {
                "timestamp": "2024-01-01T00:00:01Z",
                "tool": "run_sql",
                "sql": "SELECT bad",
                "execution_time_ms": 1.0,
                "error": "syntax",
                "user_question": "test",
            },
        ],
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["log", "--project-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "SELECT 1" in result.output
    assert "2 queries" in result.output or "queries" in result.output


def test_log_errors_only(tmp_path):
    _write_query_log(
        tmp_path,
        [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "tool": "run_sql",
                "sql": "SELECT 1",
                "execution_time_ms": 1.0,
                "row_count": 1,
            },
            {
                "timestamp": "2024-01-01T00:00:01Z",
                "tool": "run_sql",
                "sql": "SELECT bad",
                "execution_time_ms": 1.0,
                "error": "syntax",
            },
        ],
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["log", "--project-dir", str(tmp_path), "--errors"])
    assert result.exit_code == 0
    assert "SELECT bad" in result.output


def test_log_errors_filter_no_matches(tmp_path):
    _write_query_log(
        tmp_path,
        [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "tool": "run_sql",
                "sql": "SELECT 1",
                "execution_time_ms": 1.0,
            }
        ],
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["log", "--project-dir", str(tmp_path), "--errors"])
    assert result.exit_code == 0
    assert "No matching" in result.output


def test_log_sql_index_prints_raw(tmp_path):
    _write_query_log(
        tmp_path,
        [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "tool": "run_sql",
                "sql": "SELECT 42",
                "execution_time_ms": 1.0,
            }
        ],
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["log", "--project-dir", str(tmp_path), "--sql", "1"])
    assert result.exit_code == 0
    assert "SELECT 42;" in result.output


def test_log_sql_index_out_of_range(tmp_path):
    _write_query_log(
        tmp_path,
        [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "tool": "run_sql",
                "sql": "SELECT 42",
                "execution_time_ms": 1.0,
            }
        ],
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["log", "--project-dir", str(tmp_path), "--sql", "5"])
    assert result.exit_code == 0
    assert "out of range" in result.output.lower()


def test_log_sql_index_no_queries(tmp_path):
    _write_query_log(
        tmp_path,
        [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "type": "cost",
                "api_calls": 1,
                "input_tokens": 10,
                "output_tokens": 10,
                "user_question": "test",
            }
        ],
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["log", "--project-dir", str(tmp_path), "--sql", "1"])
    assert result.exit_code == 0
    assert "No SQL queries" in result.output


def test_log_cost_summary(tmp_path):
    _write_query_log(
        tmp_path,
        [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "tool": "run_sql",
                "sql": "SELECT 1",
                "execution_time_ms": 1.0,
            },
            {
                "timestamp": "2024-01-01T00:00:01Z",
                "type": "cost",
                "user_question": "test",
                "api_calls": 2,
                "input_tokens": 100,
                "output_tokens": 50,
                "estimated_cost": 0.0012,
            },
        ],
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["log", "--project-dir", str(tmp_path), "--cost"])
    assert result.exit_code == 0
    assert "Cost Summary" in result.output
    assert "0.0012" in result.output


def test_log_full_shows_questions(tmp_path):
    _write_query_log(
        tmp_path,
        [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "tool": "run_sql",
                "sql": "SELECT 1",
                "execution_time_ms": 1.0,
                "user_question": "uniqueqstring",
            }
        ],
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["log", "--project-dir", str(tmp_path), "--full"])
    assert result.exit_code == 0
    # --full exercises the branch that adds the Question column
    assert "queries" in result.output


# ---------------------------------------------------------------------------
# report subgroup
# ---------------------------------------------------------------------------


def test_report_list_empty(tmp_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["report", "list", "--project-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "No saved reports" in result.output


def test_report_list_shows_saved(project_dir):
    from datasight.web.app import ReportStore

    store = ReportStore(Path(project_dir) / ".datasight" / "reports.json")
    store.add(sql="SELECT 1 AS x", tool="run_sql", name="My Report")
    runner = CliRunner()
    result = runner.invoke(cli, ["report", "list", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "My Report" in result.output


def test_report_delete_missing(tmp_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["report", "delete", "999", "--project-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "not found" in result.output


def test_report_delete_success(project_dir):
    from datasight.web.app import ReportStore

    store = ReportStore(Path(project_dir) / ".datasight" / "reports.json")
    saved = store.add(sql="SELECT 1", tool="run_sql", name="Del Me")
    runner = CliRunner()
    result = runner.invoke(
        cli, ["report", "delete", str(saved["id"]), "--project-dir", project_dir]
    )
    assert result.exit_code == 0
    assert "deleted" in result.output.lower()


def test_report_run_missing(tmp_path):
    (tmp_path / ".env").write_text(
        "LLM_PROVIDER=ollama\nDB_MODE=duckdb\nDB_PATH=test.duckdb\n", encoding="utf-8"
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["report", "run", "999", "--project-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "not found" in result.output


def test_report_run_executes(project_dir):
    from datasight.web.app import ReportStore

    store = ReportStore(Path(project_dir) / ".datasight" / "reports.json")
    saved = store.add(
        sql="SELECT COUNT(*) AS n FROM products", tool="run_sql", name="Product count"
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["report", "run", str(saved["id"]), "--project-dir", project_dir])
    assert result.exit_code == 0
    # duckdb has 5 products in fixture
    assert "5" in result.output


def test_report_run_csv(project_dir):
    from datasight.web.app import ReportStore

    store = ReportStore(Path(project_dir) / ".datasight" / "reports.json")
    saved = store.add(sql="SELECT id FROM products ORDER BY id", tool="run_sql", name="ids")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "report",
            "run",
            str(saved["id"]),
            "--project-dir",
            project_dir,
            "--format",
            "csv",
        ],
    )
    assert result.exit_code == 0
    assert "id" in result.output


# ---------------------------------------------------------------------------
# ask command - argument validation branches (no LLM call)
# ---------------------------------------------------------------------------


def test_ask_requires_question_or_file(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["ask", "--project-dir", project_dir])
    assert result.exit_code != 0
    assert "provide a QUESTION or use --file" in result.output


def test_ask_question_and_file_mutually_exclusive(project_dir, tmp_path):
    qfile = tmp_path / "q.txt"
    qfile.write_text("hi\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(cli, ["ask", "hi", "--project-dir", project_dir, "--file", str(qfile)])
    assert result.exit_code != 0
    assert "either QUESTION or --file" in result.output


def test_ask_file_with_output_rejected(project_dir, tmp_path):
    qfile = tmp_path / "q.txt"
    qfile.write_text("hi\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "--file",
            str(qfile),
            "--output",
            str(tmp_path / "out.csv"),
        ],
    )
    assert result.exit_code != 0
    assert "--output" in result.output


def test_ask_chart_format_requires_output(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["ask", "hi", "--project-dir", project_dir, "--chart-format", "html"]
    )
    assert result.exit_code != 0
    assert "--chart-format requires --output" in result.output


def test_ask_output_dir_without_file(project_dir, tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["ask", "hi", "--project-dir", project_dir, "--output-dir", str(tmp_path)],
    )
    assert result.exit_code != 0
    assert "--output-dir can only be used with --file" in result.output


def test_ask_sql_script_with_file_rejected(project_dir, tmp_path):
    qfile = tmp_path / "q.txt"
    qfile.write_text("hi\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "--file",
            str(qfile),
            "--sql-script",
            str(tmp_path / "out.sql"),
        ],
    )
    assert result.exit_code != 0
    assert "--sql-script cannot be combined with --file" in result.output


# ---------------------------------------------------------------------------
# _load_batch_entries coverage
# ---------------------------------------------------------------------------


def test_load_batch_entries_txt(tmp_path):
    from datasight.cli import _load_batch_entries

    p = tmp_path / "q.txt"
    p.write_text("one\n\ntwo\n", encoding="utf-8")
    entries = _load_batch_entries(str(p))
    assert [e["question"] for e in entries] == ["one", "two"]


def test_load_batch_entries_jsonl(tmp_path):
    from datasight.cli import _load_batch_entries

    p = tmp_path / "q.jsonl"
    p.write_text('{"question": "one"}\n{"question": "two", "format": "csv"}\n', encoding="utf-8")
    entries = _load_batch_entries(str(p))
    assert entries[0]["question"] == "one"
    assert entries[1]["output_format"] == "csv"


def test_load_batch_entries_jsonl_invalid(tmp_path):
    import click

    from datasight.cli import _load_batch_entries

    p = tmp_path / "q.jsonl"
    p.write_text("not json\n", encoding="utf-8")
    with pytest.raises(click.ClickException):
        _load_batch_entries(str(p))


def test_load_batch_entries_jsonl_not_dict(tmp_path):
    import click

    from datasight.cli import _load_batch_entries

    p = tmp_path / "q.jsonl"
    p.write_text('["not-a-dict"]\n', encoding="utf-8")
    with pytest.raises(click.ClickException):
        _load_batch_entries(str(p))


def test_load_batch_entries_yaml(tmp_path):
    from datasight.cli import _load_batch_entries

    p = tmp_path / "q.yaml"
    p.write_text("- question: one\n- question: two\n  format: json\n", encoding="utf-8")
    entries = _load_batch_entries(str(p))
    assert entries[1]["output_format"] == "json"


def test_load_batch_entries_yaml_invalid_format(tmp_path):
    import click

    from datasight.cli import _load_batch_entries

    p = tmp_path / "q.yaml"
    p.write_text("- question: one\n  format: bogus\n", encoding="utf-8")
    with pytest.raises(click.ClickException):
        _load_batch_entries(str(p))


def test_load_batch_entries_yaml_not_list(tmp_path):
    import click

    from datasight.cli import _load_batch_entries

    p = tmp_path / "q.yaml"
    p.write_text("question: one\n", encoding="utf-8")
    with pytest.raises(click.ClickException):
        _load_batch_entries(str(p))


def test_load_batch_entries_yaml_non_mapping(tmp_path):
    import click

    from datasight.cli import _load_batch_entries

    p = tmp_path / "q.yaml"
    p.write_text("- just a string\n", encoding="utf-8")
    with pytest.raises(click.ClickException):
        _load_batch_entries(str(p))


def test_load_batch_entries_yaml_missing_question(tmp_path):
    import click

    from datasight.cli import _load_batch_entries

    p = tmp_path / "q.yaml"
    p.write_text("- name: x\n", encoding="utf-8")
    with pytest.raises(click.ClickException):
        _load_batch_entries(str(p))


def test_load_batch_entries_yaml_invalid_chart_format(tmp_path):
    import click

    from datasight.cli import _load_batch_entries

    p = tmp_path / "q.yaml"
    p.write_text("- question: one\n  chart_format: svg\n", encoding="utf-8")
    with pytest.raises(click.ClickException):
        _load_batch_entries(str(p))


# ---------------------------------------------------------------------------
# Helper coverage
# ---------------------------------------------------------------------------


def test_slugify_filename():
    from datasight.cli import _slugify_filename

    assert _slugify_filename("Hello, World!") == "hello-world"
    # Empty/whitespace input yields a fallback string (non-empty).
    assert _slugify_filename("   ")
    assert len(_slugify_filename("a" * 200)) <= 200


def test_sanitize_sql_identifier():
    from datasight.cli import _sanitize_sql_identifier

    assert _sanitize_sql_identifier("Foo-Bar 1") != ""


def test_default_data_extension():
    from datasight.cli import _default_data_extension

    assert _default_data_extension("csv") == ".csv"
    assert _default_data_extension("json") == ".json"
    assert _default_data_extension("table") in (".txt", ".tsv", ".csv")


def test_default_chart_extension():
    from datasight.cli import _default_chart_extension

    assert _default_chart_extension("html") == ".html"
    assert _default_chart_extension("json") == ".json"
    assert _default_chart_extension("png") == ".png"


def test_question_table_prefix():
    from datasight.cli import _question_table_prefix

    prefix = _question_table_prefix("What is the total generation?")
    assert isinstance(prefix, str)


def test_fmt_dist():
    from datasight.cli import _fmt_dist

    assert _fmt_dist(None) in {"-", "?"}
    assert _fmt_dist(1.23) not in {"-", "?"}


def test_format_profile_value():
    from datasight.cli import _format_profile_value

    assert _format_profile_value(None) == "?"
    assert _format_profile_value(None, default="-") == "-"
    assert _format_profile_value(1.5) != "?"


# ---------------------------------------------------------------------------
# inspect command (ephemeral, no project needed)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# trends command
# ---------------------------------------------------------------------------


def test_trends_markdown_output(project_dir, tmp_path):
    output_path = tmp_path / "trends.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "trends",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0


def test_trends_from_files(test_duckdb_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["trends", test_duckdb_path])
    assert result.exit_code == 0


def test_trends_missing_db(tmp_path, clean_env):
    (tmp_path / ".env").write_text(
        "LLM_PROVIDER=ollama\nDB_MODE=duckdb\nDB_PATH=nope.duckdb\n", encoding="utf-8"
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["trends", "--project-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "Database file not found" in result.output


# ---------------------------------------------------------------------------
# generate early error branches (no LLM needed)
# ---------------------------------------------------------------------------


def test_generate_refuses_existing_files(project_dir):
    # project_dir fixture has queries.yaml and schema_description.md already.
    runner = CliRunner()
    result = runner.invoke(cli, ["generate", "--project-dir", project_dir])
    assert result.exit_code != 0
    assert "already exist" in result.output


# ---------------------------------------------------------------------------
# verify early error branches
# ---------------------------------------------------------------------------


def test_verify_no_queries(tmp_path):
    (tmp_path / ".env").write_text(
        "LLM_PROVIDER=ollama\nOLLAMA_MODEL=x\nDB_MODE=duckdb\nDB_PATH=test.duckdb\n",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["verify", "--project-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "No queries found" in result.output


def test_verify_missing_db(tmp_path, clean_env):
    (tmp_path / ".env").write_text(
        "LLM_PROVIDER=ollama\nOLLAMA_MODEL=x\nDB_MODE=duckdb\nDB_PATH=nope.duckdb\n",
        encoding="utf-8",
    )
    (tmp_path / "queries.yaml").write_text("- question: hi\n  sql: SELECT 1\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(cli, ["verify", "--project-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "Database file not found" in result.output
