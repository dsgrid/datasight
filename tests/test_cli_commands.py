"""End-to-end CLI tests covering every `datasight` command.

The `datasight demo time-validation` command generates a fully-offline synthetic
energy consumption dataset — it produces a DuckDB database plus all four
project config files (schema_description.md, queries.yaml, measures.yaml,
time_series.yaml) and a .env that points at the database.

We use that project as the basis for exercising the rest of the CLI. The
session-scoped `tv_project` fixture runs `datasight demo time-validation`
exactly once per test session; individual tests invoke downstream commands
against the resulting project directory.

Commands that require an LLM (`ask`, `verify`, `generate`) and the `run` web
server command use `monkeypatch` to replace the external dependency with a
deterministic stub.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from datasight.cli import cli
from datasight.llm import TextBlock

from tests._env_helpers import DATASIGHT_ENV_VARS, scrub_datasight_env


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Scrub datasight env vars before AND after each test.

    load_dotenv() mutates os.environ directly, and monkeypatch.delenv only
    tracks vars that existed at setup. Scrub on teardown too so vars set
    during the test (via CliRunner → load_dotenv) don't leak into later
    test files.
    """
    for key in DATASIGHT_ENV_VARS:
        monkeypatch.delenv(key, raising=False)
    yield
    scrub_datasight_env()


# ---------------------------------------------------------------------------
# Session fixture: generate the time-validation project once
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def tv_project(tmp_path_factory) -> str:
    """Generate the time-validation demo project once per session.

    This also exercises the `datasight demo time-validation` command end-to-end.
    Returns the project directory path.
    """
    project_dir = tmp_path_factory.mktemp("tv-demo")
    runner = CliRunner()
    result = runner.invoke(cli, ["demo", "time-validation", str(project_dir)])
    assert result.exit_code == 0, f"demo time-validation failed: {result.output}"
    assert "Demo project ready!" in result.output

    # Verify all expected project artefacts are present.
    for name in (
        ".env",
        "schema_description.md",
        "queries.yaml",
        "measures.yaml",
        "time_series.yaml",
        "time_validation_demo.duckdb",
    ):
        assert (project_dir / name).exists(), f"missing {name}"

    # Minimal .env produced by the demo uses DB_MODE=duckdb — but the demo
    # doesn't seed an LLM provider. Append a stub provider so downstream
    # commands that load settings don't fail validation.
    env_text = (project_dir / ".env").read_text(encoding="utf-8")
    if "LLM_PROVIDER" not in env_text:
        (project_dir / ".env").write_text(
            env_text + "LLM_PROVIDER=ollama\nOLLAMA_MODEL=qwen2.5:7b\n",
            encoding="utf-8",
        )

    return str(project_dir)


@pytest.fixture()
def tv_project_isolated(tv_project, tmp_path, monkeypatch):
    """A fresh copy of the time-validation project for tests that mutate it.

    Only the config files are copied — the 100MB DuckDB is referenced by
    absolute path via .env rewrite.
    """
    src = Path(tv_project)
    dst = tmp_path / "tv-copy"
    dst.mkdir()
    for name in (
        "schema_description.md",
        "queries.yaml",
        "measures.yaml",
        "time_series.yaml",
    ):
        (dst / name).write_text((src / name).read_text(encoding="utf-8"), encoding="utf-8")
    # Rewrite .env to point at the shared DB and add an LLM stub provider.
    (dst / ".env").write_text(
        "DB_MODE=duckdb\n"
        f"DB_PATH={src / 'time_validation_demo.duckdb'}\n"
        "LLM_PROVIDER=ollama\n"
        "OLLAMA_MODEL=qwen2.5:7b\n",
        encoding="utf-8",
    )
    return str(dst)


# ---------------------------------------------------------------------------
# Top-level help
# ---------------------------------------------------------------------------


def test_cli_help_lists_all_commands():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    for cmd in (
        "run",
        "ask",
        "init",
        "generate",
        "verify",
        "demo",
        "profile",
        "measures",
        "quality",
        "integrity",
        "distribution",
        "validate",
        "audit-report",
        "dimensions",
        "trends",
        "recipes",
        "inspect",
        "doctor",
        "export",
        "log",
        "report",
    ):
        assert cmd in result.output, f"command {cmd!r} missing from --help"


def test_cli_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert "version" in result.output.lower() or "." in result.output


# ---------------------------------------------------------------------------
# demo (group) + demo time-validation
# ---------------------------------------------------------------------------


def test_demo_group_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["demo", "--help"])
    assert result.exit_code == 0
    for sub in ("time-validation", "eia-generation", "dsgrid-tempo"):
        assert sub in result.output


def test_demo_time_validation_generates_project(tv_project):
    """The tv_project fixture already invoked `demo time-validation`. Assert the
    project has the expected artefacts."""
    p = Path(tv_project)
    assert (p / "time_validation_demo.duckdb").stat().st_size > 1_000_000
    schema = (p / "schema_description.md").read_text(encoding="utf-8")
    assert "hourly_consumption" in schema
    queries = (p / "queries.yaml").read_text(encoding="utf-8")
    assert "SELECT" in queries.upper()


# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------


def test_doctor_on_tv_project(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["doctor", "--project-dir", tv_project])
    assert result.exit_code == 0
    assert "doctor" in result.output.lower() or "checks" in result.output.lower()


def test_doctor_json_output(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["doctor", "--project-dir", tv_project, "--format", "json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "checks" in data


# ---------------------------------------------------------------------------
# profile
# ---------------------------------------------------------------------------


def test_profile_dataset_scope(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["profile", "--project-dir", tv_project])
    assert result.exit_code == 0
    assert "hourly_consumption" in result.output


def test_profile_single_table(tv_project):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["profile", "--project-dir", tv_project, "--table", "hourly_consumption"],
    )
    assert result.exit_code == 0


def test_profile_single_column(tv_project):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "profile",
            "--project-dir",
            tv_project,
            "--column",
            "hourly_consumption.consumption_mwh",
        ],
    )
    assert result.exit_code == 0


def test_profile_json_output(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["profile", "--project-dir", tv_project, "--format", "json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "table_count" in data or "tables" in data or "largest_tables" in data


# ---------------------------------------------------------------------------
# quality
# ---------------------------------------------------------------------------


def test_quality_on_tv_project(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["quality", "--project-dir", tv_project])
    assert result.exit_code == 0


def test_quality_json_output(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["quality", "--project-dir", tv_project, "--format", "json"])
    assert result.exit_code == 0
    json.loads(result.output)  # valid JSON


def test_quality_markdown_output(tv_project, tmp_path):
    out_path = tmp_path / "quality.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "quality",
            "--project-dir",
            tv_project,
            "--format",
            "markdown",
            "--output",
            str(out_path),
        ],
    )
    assert result.exit_code == 0
    assert out_path.exists()


# ---------------------------------------------------------------------------
# measures
# ---------------------------------------------------------------------------


def test_measures_on_tv_project(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["measures", "--project-dir", tv_project])
    assert result.exit_code == 0
    assert "consumption_mwh" in result.output or "Measure" in result.output


def test_measures_json_output(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["measures", "--project-dir", tv_project, "--format", "json"])
    assert result.exit_code == 0
    json.loads(result.output)


# ---------------------------------------------------------------------------
# dimensions
# ---------------------------------------------------------------------------


def test_dimensions_on_tv_project(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["dimensions", "--project-dir", tv_project])
    assert result.exit_code == 0


def test_dimensions_json_output(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["dimensions", "--project-dir", tv_project, "--format", "json"])
    assert result.exit_code == 0
    json.loads(result.output)


# ---------------------------------------------------------------------------
# trends
# ---------------------------------------------------------------------------


def test_trends_on_tv_project(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["trends", "--project-dir", tv_project])
    assert result.exit_code == 0


def test_trends_json_output(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["trends", "--project-dir", tv_project, "--format", "json"])
    assert result.exit_code == 0
    json.loads(result.output)


# ---------------------------------------------------------------------------
# integrity / distribution / validate / audit-report
# ---------------------------------------------------------------------------


def test_integrity_on_tv_project(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["integrity", "--project-dir", tv_project])
    assert result.exit_code == 0


def test_distribution_on_tv_project(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["distribution", "--project-dir", tv_project])
    assert result.exit_code == 0


def test_distribution_with_column(tv_project):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "distribution",
            "--project-dir",
            tv_project,
            "--column",
            "hourly_consumption.consumption_mwh",
        ],
    )
    assert result.exit_code == 0


def test_validate_scaffold_generates_rules(tv_project_isolated):
    """`validate --scaffold` creates a validations.yaml from schema."""
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--project-dir", tv_project_isolated, "--scaffold"])
    assert result.exit_code == 0
    assert (Path(tv_project_isolated) / "validation.yaml").exists()


def test_validate_runs_rules(tv_project_isolated):
    """Scaffold then run validate."""
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--project-dir", tv_project_isolated, "--scaffold"])
    assert result.exit_code == 0
    result = runner.invoke(cli, ["validate", "--project-dir", tv_project_isolated])
    assert result.exit_code == 0


def test_audit_report_on_tv_project(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["audit-report", "--project-dir", tv_project])
    assert result.exit_code == 0


def test_audit_report_html_output(tv_project, tmp_path):
    out_path = tmp_path / "audit.html"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "audit-report",
            "--project-dir",
            tv_project,
            "--format",
            "html",
            "--output",
            str(out_path),
        ],
    )
    assert result.exit_code == 0
    assert out_path.exists()
    assert "<html" in out_path.read_text(encoding="utf-8").lower()


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------


def test_inspect_duckdb_file(tv_project):
    db_path = str(Path(tv_project) / "time_validation_demo.duckdb")
    runner = CliRunner()
    result = runner.invoke(cli, ["inspect", db_path])
    assert result.exit_code == 0
    assert "hourly_consumption" in result.output


def test_inspect_json_output(tv_project):
    db_path = str(Path(tv_project) / "time_validation_demo.duckdb")
    runner = CliRunner()
    result = runner.invoke(cli, ["inspect", db_path, "--format", "json"])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# recipes
# ---------------------------------------------------------------------------


def test_recipes_list(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["recipes", "list", "--project-dir", tv_project])
    assert result.exit_code == 0


def test_recipes_run_with_stubbed_pipeline(monkeypatch, tv_project):
    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text=f"recipe answer for: {kwargs['question'][:30]}",
            tool_results=[],
            total_input_tokens=0,
            total_output_tokens=0,
            api_calls=0,
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)
    runner = CliRunner()
    result = runner.invoke(cli, ["recipes", "run", "1", "--project-dir", tv_project])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# ask (mocked agent pipeline)
# ---------------------------------------------------------------------------


def test_ask_with_stubbed_pipeline(monkeypatch, tv_project):
    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text=f"stubbed answer for: {kwargs['question']}",
            tool_results=[],
            total_input_tokens=0,
            total_output_tokens=0,
            api_calls=0,
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)
    runner = CliRunner()
    result = runner.invoke(cli, ["ask", "How many states are there?", "--project-dir", tv_project])
    assert result.exit_code == 0
    assert "stubbed answer for: How many states are there?" in result.output


# ---------------------------------------------------------------------------
# generate (mocked LLM)
# ---------------------------------------------------------------------------


def test_generate_rewrites_project_files(monkeypatch, tv_project_isolated):
    """Regenerate schema_description.md + queries.yaml using a stubbed LLM."""
    fake_response = (
        "--- schema_description.md ---\n"
        "# Generated Schema\n\n"
        "Contains hourly consumption.\n\n"
        "--- queries.yaml ---\n"
        "- question: How many rows?\n"
        "  sql: SELECT COUNT(*) FROM hourly_consumption\n"
    )

    class _StubClient:
        async def create_message(self, **kwargs):
            return SimpleNamespace(
                content=[TextBlock(fake_response)],
                stop_reason="end_turn",
                usage=SimpleNamespace(input_tokens=1, output_tokens=1),
            )

    monkeypatch.setattr("datasight.cli.create_llm_client", lambda **kwargs: _StubClient())

    runner = CliRunner()
    result = runner.invoke(cli, ["generate", "--project-dir", tv_project_isolated, "--overwrite"])
    assert result.exit_code == 0, result.output
    assert "Created:" in result.output
    schema = (Path(tv_project_isolated) / "schema_description.md").read_text(encoding="utf-8")
    assert "Generated Schema" in schema


# ---------------------------------------------------------------------------
# run (mocked uvicorn)
# ---------------------------------------------------------------------------


def test_run_command_invokes_uvicorn(monkeypatch, tv_project):
    """`datasight run` wires up settings and starts uvicorn.

    We mock uvicorn.run so the test doesn't block on a live server, and verify
    the command parses its arguments and prints the startup banner.
    """
    captured = {}

    def fake_uvicorn_run(app, *, host=None, port=None, uds=None, log_level):  # noqa: ARG001
        captured["app"] = app
        captured["host"] = host
        captured["port"] = port
        captured["uds"] = uds

    # The CLI does `import uvicorn` inside the function body, so patch on the
    # real module.
    import uvicorn

    monkeypatch.setattr(uvicorn, "run", fake_uvicorn_run)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "run",
            "--project-dir",
            tv_project,
            "--port",
            "9999",
            "--host",
            "127.0.0.1",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured["port"] == 9999
    assert captured["host"] == "127.0.0.1"
    assert captured["uds"] is None
    assert captured["app"] == "datasight.web.app:app"


def test_run_command_defaults_to_loopback(monkeypatch, tv_project):
    """`datasight run` should default to a loopback-only TCP bind."""
    captured = {}

    def fake_uvicorn_run(app, *, host=None, port=None, uds=None, log_level):  # noqa: ARG001
        captured["host"] = host
        captured["port"] = port
        captured["uds"] = uds

    import uvicorn

    monkeypatch.setattr(uvicorn, "run", fake_uvicorn_run)

    runner = CliRunner()
    result = runner.invoke(cli, ["run", "--project-dir", tv_project])
    assert result.exit_code == 0, result.output
    assert captured["host"] == "127.0.0.1"
    assert captured["uds"] is None


def test_run_command_supports_unix_socket(monkeypatch, tv_project, tmp_path):
    """`datasight run --unix-socket` should switch uvicorn to UDS mode."""
    captured = {}
    socket_path = tmp_path / "datasight.sock"

    def fake_uvicorn_run(app, *, host=None, port=None, uds=None, log_level):  # noqa: ARG001
        captured["host"] = host
        captured["port"] = port
        captured["uds"] = uds

    import uvicorn

    monkeypatch.setattr(uvicorn, "run", fake_uvicorn_run)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["run", "--project-dir", tv_project, "--unix-socket", str(socket_path)],
    )
    assert result.exit_code == 0, result.output
    assert captured["host"] is None
    assert captured["port"] is None
    assert captured["uds"] == str(socket_path)


def test_run_command_rejects_port_with_unix_socket(tv_project, tmp_path):
    """`--port` is invalid when the server is running on a UNIX socket."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "run",
            "--project-dir",
            tv_project,
            "--port",
            "9999",
            "--unix-socket",
            str(tmp_path / "datasight.sock"),
        ],
    )
    assert result.exit_code != 0
    assert "--port cannot be used with --unix-socket" in result.output


# ---------------------------------------------------------------------------
# log
# ---------------------------------------------------------------------------


def test_log_empty(tv_project):
    """Log command on a fresh project — no queries yet."""
    runner = CliRunner()
    result = runner.invoke(cli, ["log", "--project-dir", tv_project])
    # Empty log is not an error; just produces a "no queries" message.
    assert result.exit_code in (0, 1)


def test_log_with_entries(tv_project_isolated):
    """Seed a query_log.jsonl then view it with `datasight log`."""
    log_dir = Path(tv_project_isolated) / ".datasight"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "query_log.jsonl"
    log_file.write_text(
        '{"timestamp":"2024-01-01T00:00:00+00:00","session_id":"s1",'
        '"user_question":"count","tool":"run_sql","sql":"SELECT 1",'
        '"execution_time_ms":1.0,"row_count":1,"column_count":1,"error":null}\n',
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["log", "--project-dir", tv_project_isolated])
    assert result.exit_code == 0
    assert "SELECT 1" in result.output or "count" in result.output


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------


def test_export_lists_empty_sessions(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["export", "list", "--project-dir", tv_project])
    # No sessions yet — exits 0 with a "no conversations" message.
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# report (list / run / delete)
# ---------------------------------------------------------------------------


def test_report_list_empty(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["report", "list", "--project-dir", tv_project])
    assert result.exit_code == 0


def test_report_run_missing(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["report", "run", "999", "--project-dir", tv_project])
    assert result.exit_code != 0


def test_report_delete_missing(tv_project):
    runner = CliRunner()
    result = runner.invoke(cli, ["report", "delete", "999", "--project-dir", tv_project])
    # ReportStore.delete silently no-ops on missing IDs, so exit may be 0.
    assert result.exit_code in (0, 1)


def test_report_run_saved_report(tv_project_isolated):
    """Seed a saved report, then run it."""
    datasight_dir = Path(tv_project_isolated) / ".datasight"
    datasight_dir.mkdir(parents=True, exist_ok=True)
    reports = [
        {
            "id": 1,
            "name": "State row counts",
            "tool": "run_sql",
            "sql": "SELECT state, COUNT(*) AS n FROM hourly_consumption GROUP BY state LIMIT 5",
        }
    ]
    (datasight_dir / "reports.json").write_text(json.dumps(reports), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(cli, ["report", "list", "--project-dir", tv_project_isolated])
    assert result.exit_code == 0
    assert "State row counts" in result.output

    result = runner.invoke(cli, ["report", "run", "1", "--project-dir", tv_project_isolated])
    assert result.exit_code == 0

    result = runner.invoke(cli, ["report", "delete", "1", "--project-dir", tv_project_isolated])
    assert result.exit_code == 0
