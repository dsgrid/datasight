"""Tests for CLI profiling and batch ask helpers."""

import json
from pathlib import Path
from types import SimpleNamespace

from click.testing import CliRunner

from datasight.cli import cli


def test_profile_dataset(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["profile", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Dataset Profile" in result.output
    assert "Largest Tables" in result.output
    assert "Date Coverage" in result.output
    assert "Measure Candidates" in result.output


def test_profile_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["profile", "--project-dir", project_dir, "--table", "orders"])
    assert result.exit_code == 0
    assert "Table Profile" in result.output
    assert "orders" in result.output
    assert "Date Columns" in result.output
    assert "Numeric Columns" in result.output
    assert "Text Dimensions" in result.output


def test_profile_column(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["profile", "--project-dir", project_dir, "--column", "orders.order_date"],
    )
    assert result.exit_code == 0
    assert "Column Profile" in result.output
    assert "orders.order_date" in result.output
    assert "Date Coverage" in result.output


def test_profile_dataset_markdown_includes_sections(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["profile", "--project-dir", project_dir, "--format", "markdown"])
    assert result.exit_code == 0
    assert "# Dataset Profile" in result.output
    assert "## Largest Tables" in result.output
    assert "## Date Coverage" in result.output


def test_profile_table_markdown_includes_numeric_and_text_sections(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["profile", "--project-dir", project_dir, "--table", "orders", "--format", "markdown"],
    )
    assert result.exit_code == 0
    assert "# Table Profile: orders" in result.output
    assert "## Date Columns" in result.output
    assert "## Text Dimensions" in result.output


def test_profile_column_markdown_includes_date_coverage(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "profile",
            "--project-dir",
            project_dir,
            "--column",
            "orders.order_date",
            "--format",
            "markdown",
        ],
    )
    assert result.exit_code == 0
    assert "# Column Profile: orders.order_date" in result.output
    assert "## Date Coverage" in result.output


def test_profile_markdown_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "profile.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "profile",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    assert output_path.exists()
    assert "# Dataset Profile" in output_path.read_text(encoding="utf-8")


def test_profile_json_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "profile.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "profile",
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
    assert data["table_count"] >= 1


def test_quality_table_output(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["quality", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Dataset Quality Audit" in result.output
    assert "Date Coverage" in result.output
    assert "Notes" in result.output


def test_quality_table_scope(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["quality", "--project-dir", project_dir, "--table", "orders"])
    assert result.exit_code == 0
    assert "Dataset Quality Audit" in result.output
    assert "1" in result.output
    assert "orders.order_date" in result.output


def test_quality_table_scope_missing_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["quality", "--project-dir", project_dir, "--table", "missing_table"]
    )
    assert result.exit_code != 0
    assert "Table not found: missing_table" in result.output


def test_quality_markdown_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "quality.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "quality",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    text = output_path.read_text(encoding="utf-8")
    assert "# Dataset Quality Audit" in text
    assert "## Date Coverage" in text


def test_quality_json_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "quality.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "quality",
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
    assert data["table_count"] >= 1
    assert "notes" in data


def test_dimensions_table_output(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["dimensions", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Dimension Overview" in result.output
    assert "Dimension Candidates" in result.output
    assert "Suggested Breakdowns" in result.output


def test_dimensions_table_scope(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["dimensions", "--project-dir", project_dir, "--table", "orders"])
    assert result.exit_code == 0
    assert "Dimension Overview" in result.output
    assert "Tables scanned" in result.output
    assert "Suggested Breakdowns" in result.output


def test_dimensions_table_scope_missing_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["dimensions", "--project-dir", project_dir, "--table", "missing_table"]
    )
    assert result.exit_code != 0
    assert "Table not found: missing_table" in result.output


def test_dimensions_markdown_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "dimensions.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "dimensions",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    text = output_path.read_text(encoding="utf-8")
    assert "# Dimension Overview" in text
    assert "## Suggested Breakdowns" in text


def test_trends_table_output(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["trends", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Trend Overview" in result.output
    assert "Trend Candidates" in result.output
    assert "Chart Recommendations" in result.output


def test_trends_table_scope(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["trends", "--project-dir", project_dir, "--table", "orders"])
    assert result.exit_code == 0
    assert "Trend Overview" in result.output
    assert "orders" in result.output
    assert "order_date" in result.output


def test_trends_table_scope_missing_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["trends", "--project-dir", project_dir, "--table", "missing_table"]
    )
    assert result.exit_code != 0
    assert "Table not found: missing_table" in result.output


def test_trends_json_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "trends.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["trends", "--project-dir", project_dir, "--format", "json", "--output", str(output_path)],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert data["table_count"] >= 1
    assert "trend_candidates" in data


def test_recipes_table_output(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["recipes", "list", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Prompt Recipes" in result.output
    assert "ID" in result.output
    assert "Orientation" in result.output
    assert "Why" in result.output


def test_recipes_table_scope(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["recipes", "list", "--project-dir", project_dir, "--table", "orders"]
    )
    assert result.exit_code == 0
    assert "Prompt Recipes" in result.output
    assert "orders" in result.output


def test_recipes_table_scope_missing_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["recipes", "list", "--project-dir", project_dir, "--table", "missing_table"]
    )
    assert result.exit_code != 0
    assert "Table not found: missing_table" in result.output


def test_recipes_markdown_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "recipes.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "recipes",
            "list",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    text = output_path.read_text(encoding="utf-8")
    assert "# Prompt Recipes" in text
    assert "## [1] Profile the biggest tables" in text
    assert "- Why this recipe:" in text


def test_recipes_list_json_includes_ids(project_dir, tmp_path):
    output_path = tmp_path / "recipes.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "recipes",
            "list",
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
    assert data[0]["id"] == 1
    assert "prompt" in data[0]


def test_recipes_run_executes_selected_prompt(monkeypatch, project_dir):
    captured = {}

    async def fake_run_ask_pipeline(**kwargs):
        captured["question"] = kwargs["question"]
        return SimpleNamespace(text="recipe answer", tool_results=[])

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(cli, ["recipes", "run", "1", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Running recipe [1]" in result.output
    assert captured["question"].startswith("Profile the biggest")


def test_recipes_run_missing_id(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["recipes", "run", "999", "--project-dir", project_dir])
    assert result.exit_code != 0
    assert "Recipe 999 not found." in result.output


def test_ask_file_runs_all_questions(monkeypatch, project_dir, tmp_path):
    questions_path = tmp_path / "questions.txt"
    questions_path.write_text(
        "How many orders are there?\n\nList all products.\n", encoding="utf-8"
    )

    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text=f"answer: {kwargs['question']}",
            tool_results=[],
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["ask", "--project-dir", project_dir, "--file", str(questions_path)],
    )
    assert result.exit_code == 0
    assert "[1/2] How many orders are there?" in result.output
    assert "[2/2] List all products." in result.output
    assert "Batch complete: 2/2 succeeded." in result.output


def test_ask_file_output_dir_writes_artifacts(monkeypatch, project_dir, tmp_path):
    questions_path = tmp_path / "questions.txt"
    questions_path.write_text("How many orders are there?\n", encoding="utf-8")
    output_dir = tmp_path / "batch-output"

    class FakeFrame:
        empty = False

        def to_csv(self, index=False):  # noqa: ARG002
            return "count\n10\n"

        def to_string(self, index=False):  # noqa: ARG002
            return " count\n    10"

        def to_json(self, orient="records", indent=2):  # noqa: ARG002
            return '[{"count":10}]'

    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text=f"answer: {kwargs['question']}",
            tool_results=[
                SimpleNamespace(
                    df=FakeFrame(),
                    plotly_spec={"data": [{"type": "bar"}], "layout": {"title": "Chart"}},
                    meta={"title": "Orders"},
                )
            ],
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "--file",
            str(questions_path),
            "--output-dir",
            str(output_dir),
            "--chart-format",
            "json",
            "--format",
            "csv",
        ],
    )
    assert result.exit_code == 0
    assert "Saved:" in result.output
    answer_file = output_dir / "01-how-many-orders-are-there.answer.txt"
    data_file = output_dir / "01-how-many-orders-are-there.result-1.csv"
    chart_file = output_dir / "01-how-many-orders-are-there.chart-1.json"
    assert answer_file.exists()
    assert data_file.exists()
    assert chart_file.exists()
    assert "answer: How many orders are there?" in answer_file.read_text(encoding="utf-8")
    assert "count" in data_file.read_text(encoding="utf-8")
    assert '"type": "bar"' in chart_file.read_text(encoding="utf-8")


def test_ask_yaml_file_applies_per_entry_overrides(monkeypatch, project_dir, tmp_path):
    questions_path = tmp_path / "questions.yaml"
    questions_path.write_text(
        (
            "- question: How many orders are there?\n"
            "  format: json\n"
            "  chart_format: json\n"
            "  name: orders-summary\n"
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "yaml-batch-output"

    class FakeFrame:
        empty = False

        def to_csv(self, index=False):  # noqa: ARG002
            return "count\n10\n"

        def to_string(self, index=False):  # noqa: ARG002
            return " count\n    10"

        def to_json(self, orient="records", indent=2):  # noqa: ARG002
            return '[{"count":10}]'

    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text="yaml answer",
            tool_results=[
                SimpleNamespace(
                    df=FakeFrame(),
                    plotly_spec={"data": [{"type": "line"}], "layout": {"title": "Chart"}},
                    meta={"title": "Orders"},
                )
            ],
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "--file",
            str(questions_path),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0
    data_file = output_dir / "01-orders-summary.result-1.json"
    chart_file = output_dir / "01-orders-summary.chart-1.json"
    assert data_file.exists()
    assert chart_file.exists()
    assert '"count":10' in data_file.read_text(encoding="utf-8")
    assert '"type": "line"' in chart_file.read_text(encoding="utf-8")


def test_ask_yaml_file_supports_output_base_override(monkeypatch, project_dir, tmp_path):
    questions_path = tmp_path / "questions.yaml"
    questions_path.write_text(
        (
            "- question: How many orders are there?\n"
            "  format: json\n"
            "  chart_format: json\n"
            "  output: reports/orders-summary\n"
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "batch-output"

    class FakeFrame:
        empty = False

        def to_csv(self, index=False):  # noqa: ARG002
            return "count\n10\n"

        def to_string(self, index=False):  # noqa: ARG002
            return " count\n    10"

        def to_json(self, orient="records", indent=2):  # noqa: ARG002
            return '[{"count":10}]'

    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text="yaml answer",
            tool_results=[
                SimpleNamespace(
                    df=FakeFrame(),
                    plotly_spec={"data": [{"type": "line"}], "layout": {"title": "Chart"}},
                    meta={"title": "Orders"},
                )
            ],
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "--file",
            str(questions_path),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0
    data_file = output_dir / "reports" / "orders-summary.result-1.json"
    chart_file = output_dir / "reports" / "orders-summary.chart-1.json"
    answer_file = output_dir / "reports" / "orders-summary.answer.txt"
    assert answer_file.exists()
    assert data_file.exists()
    assert chart_file.exists()


def test_ask_jsonl_file_supports_output_without_output_dir(monkeypatch, project_dir, tmp_path):
    questions_path = tmp_path / "questions.jsonl"
    target_base = tmp_path / "named-output" / "job"
    questions_path.write_text(
        json.dumps(
            {
                "question": "How many orders are there?",
                "format": "json",
                "output": str(target_base),
            }
        )
        + "\n",
        encoding="utf-8",
    )

    class FakeFrame:
        empty = False

        def to_csv(self, index=False):  # noqa: ARG002
            return "count\n10\n"

        def to_string(self, index=False):  # noqa: ARG002
            return " count\n    10"

        def to_json(self, orient="records", indent=2):  # noqa: ARG002
            return '[{"count":10}]'

    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text="jsonl answer",
            tool_results=[SimpleNamespace(df=FakeFrame(), plotly_spec=None, meta={})],
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["ask", "--project-dir", project_dir, "--file", str(questions_path)],
    )
    assert result.exit_code == 0
    assert Path(str(target_base) + ".answer.txt").exists()
    assert Path(str(target_base) + ".result-1.json").exists()


def test_ask_yaml_file_rejects_invalid_format(project_dir, tmp_path):
    questions_path = tmp_path / "questions.yaml"
    questions_path.write_text(
        ("- question: How many orders are there?\n  format: parquet\n"),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["ask", "--project-dir", project_dir, "--file", str(questions_path)],
    )
    assert result.exit_code != 0
    assert "invalid format" in result.output


def test_ask_jsonl_file_rejects_invalid_chart_format(project_dir, tmp_path):
    questions_path = tmp_path / "questions.jsonl"
    questions_path.write_text(
        json.dumps(
            {
                "question": "How many orders are there?",
                "chart_format": "svg",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["ask", "--project-dir", project_dir, "--file", str(questions_path)],
    )
    assert result.exit_code != 0
    assert "invalid chart_format" in result.output


def test_doctor_reports_healthy_project(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["doctor", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "datasight doctor" in result.output
    assert "Database connectivity" in result.output
    assert "schema_description.md" in result.output


def test_doctor_json_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "doctor.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["doctor", "--project-dir", project_dir, "--format", "json", "--output", str(output_path)],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert data["failures"] == 0
    assert any(check["name"] == "Database connectivity" for check in data["checks"])


def test_doctor_markdown_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "doctor.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "doctor",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    text = output_path.read_text(encoding="utf-8")
    assert "# datasight doctor" in text
    assert "## Checks" in text


def test_doctor_fails_when_required_files_missing(tmp_path, test_duckdb_path):
    (tmp_path / ".env").write_text(
        (
            "LLM_PROVIDER=ollama\n"
            "OLLAMA_MODEL=qwen3.5:35b-a3b\n"
            "DB_MODE=duckdb\n"
            f"DB_PATH={test_duckdb_path}\n"
        ),
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["doctor", "--project-dir", str(tmp_path)])
    assert result.exit_code == 1
    assert "queries.yaml" in result.output
    assert "FAIL" in result.output


# ---------------------------------------------------------------------------
# Ask SQL helpers (--print-sql / --sql-script)
# ---------------------------------------------------------------------------


def _make_sql_result(text="answer", queries=None):
    """Build a fake AgentResult-shaped namespace for SQL helper tests."""
    queries = queries or []
    tool_results = []
    for q in queries:
        tool_results.append(
            SimpleNamespace(
                df=None,
                plotly_spec=None,
                meta={
                    "tool": q.get("tool", "run_sql"),
                    "sql": q["sql"],
                    "formatted_sql": q.get("formatted_sql", q["sql"]),
                    "error": q.get("error"),
                },
            )
        )
    return SimpleNamespace(text=text, tool_results=tool_results)


def test_sanitize_sql_identifier_basic():
    from datasight.cli import _sanitize_sql_identifier

    assert _sanitize_sql_identifier("Top 5 states by generation") == "top_5_states_by_generation"
    assert _sanitize_sql_identifier("  ??  ") == "query"
    assert _sanitize_sql_identifier("123 widgets") == "q_123_widgets"
    # Long input is capped at the 32-char slug limit
    assert len(_sanitize_sql_identifier("a" * 200)) <= 32


def test_sanitize_sql_identifier_strips_non_ascii():
    """Non-ASCII alphanumerics must be replaced — Postgres folds unquoted
    identifiers and truncates at 63 *bytes*, so multi-byte UTF-8 chars
    can blow that budget on a per-character truncation scheme.
    """
    from datasight.cli import _question_table_prefix, _sanitize_sql_identifier

    # Japanese, French, accents, emoji — all stripped to underscores then collapsed.
    assert _sanitize_sql_identifier("日本語の質問") == "query"
    assert _sanitize_sql_identifier("café revenue") == "caf_revenue"
    assert _sanitize_sql_identifier("naïve résumé") == "na_ve_r_sum"
    # Mixed: ASCII parts survive, non-ASCII parts are dropped.
    assert _sanitize_sql_identifier("top 5 日本") == "top_5"
    # The full table prefix (slug + hash) for a worst-case 200-byte
    # multi-byte question must still fit within Postgres' 63-byte limit
    # even after a `_<n>` suffix is appended.
    huge = "日" * 200
    prefix = _question_table_prefix(huge)
    assert len(f"{prefix}_999".encode("utf-8")) <= 63


def test_question_table_prefix_distinguishes_long_questions():
    """Two long questions sharing the same first sanitized chars must not collide."""
    from datasight.cli import _question_table_prefix, _sanitize_sql_identifier

    q1 = (
        "What are the top 10 products by revenue in the western region for "
        "the last fiscal quarter that we tracked"
    )
    q2 = (
        "What are the top 10 products by revenue in the western region for "
        "the entire calendar year of 2024"
    )
    # Sanity-check the precondition: the bare slugs DO collide.
    assert _sanitize_sql_identifier(q1) == _sanitize_sql_identifier(q2)
    # The full prefix must NOT collide, thanks to the hash suffix.
    assert _question_table_prefix(q1) != _question_table_prefix(q2)
    # And the prefix must be deterministic for the same question.
    assert _question_table_prefix(q1) == _question_table_prefix(q1)


def test_build_sql_script_duckdb_create_or_replace():
    from datasight.cli import _build_sql_script, _question_table_prefix

    result = _make_sql_result(
        queries=[
            {"sql": "SELECT 1 AS x", "formatted_sql": "SELECT 1 AS x"},
            {"sql": "SELECT 2 AS y", "formatted_sql": "SELECT 2 AS y"},
        ]
    )
    prefix = _question_table_prefix("Top widgets")
    script = _build_sql_script(result, "Top widgets", "duckdb")
    assert "-- Question: Top widgets" in script
    assert "-- Dialect: duckdb" in script
    assert f"CREATE OR REPLACE TABLE {prefix}_1 AS" in script
    assert "SELECT 1 AS x;" in script
    assert f"CREATE OR REPLACE TABLE {prefix}_2 AS" in script
    assert "SELECT 2 AS y;" in script
    assert "DROP TABLE" not in script


def test_build_sql_script_postgres_uses_drop_then_create():
    from datasight.cli import _build_sql_script, _question_table_prefix

    result = _make_sql_result(queries=[{"sql": "SELECT * FROM t"}])
    prefix = _question_table_prefix("list rows")
    script = _build_sql_script(result, "list rows", "postgres")
    assert f"DROP TABLE IF EXISTS {prefix}_1;" in script
    assert f"CREATE TABLE {prefix}_1 AS" in script
    assert "CREATE OR REPLACE TABLE" not in script


def test_build_sql_script_skips_errored_queries():
    from datasight.cli import _build_sql_script, _question_table_prefix

    result = _make_sql_result(
        queries=[
            {"sql": "SELECT 1", "error": "boom"},
            {"sql": "SELECT 2"},
        ]
    )
    prefix = _question_table_prefix("q")
    script = _build_sql_script(result, "q", "duckdb")
    assert "-- Skipped attempt (errored, not materialized):" in script
    assert "--   error: boom" in script
    # Failed attempts must NOT consume a table-name index — the lone
    # successful query lands on _1, not _2.
    assert f"CREATE OR REPLACE TABLE {prefix}_1 AS" in script
    assert f"CREATE OR REPLACE TABLE {prefix}_2 AS" not in script


def test_build_sql_script_table_names_stable_across_retries():
    """Same final result must land on the same table name regardless of
    how many failed attempts preceded it — otherwise rerunning the same
    question against a different agent attempt sequence leaves stale
    tables behind.
    """
    from datasight.cli import _build_sql_script, _question_table_prefix

    # Run A: agent succeeds on the first try.
    result_a = _make_sql_result(queries=[{"sql": "SELECT final"}])
    # Run B: agent retries twice before succeeding with the same query.
    result_b = _make_sql_result(
        queries=[
            {"sql": "SELECT bad1", "error": "syntax err"},
            {"sql": "SELECT bad2", "error": "missing col"},
            {"sql": "SELECT final"},
        ]
    )
    prefix = _question_table_prefix("same q")
    script_a = _build_sql_script(result_a, "same q", "duckdb")
    script_b = _build_sql_script(result_b, "same q", "duckdb")
    # Both runs must materialize the final result on _1.
    assert f"CREATE OR REPLACE TABLE {prefix}_1 AS" in script_a
    assert f"CREATE OR REPLACE TABLE {prefix}_1 AS" in script_b
    # And neither should leak a _2 / _3 from the failed attempts.
    assert f"CREATE OR REPLACE TABLE {prefix}_2 AS" not in script_b
    assert f"CREATE OR REPLACE TABLE {prefix}_3 AS" not in script_b


def test_build_sql_script_no_queries():
    from datasight.cli import _build_sql_script

    script = _build_sql_script(_make_sql_result(), "anything", "duckdb")
    assert "(no SQL queries were executed)" in script


def test_build_sql_script_escapes_newlines_in_question():
    """A newline in the question must not escape the header comment."""
    from datasight.cli import _build_sql_script

    result = _make_sql_result(queries=[{"sql": "SELECT 1"}])
    script = _build_sql_script(result, "top rows\nDROP TABLE important;", "duckdb")
    # Every non-empty line above the generated DDL must be a SQL comment —
    # in particular, the malicious second line must still be commented.
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        # The only non-comment lines should be the generated CREATE/SELECT.
        assert stripped.startswith(("CREATE ", "SELECT ")), f"leaked line: {line!r}"
    assert "--   DROP TABLE important;" in script


def test_build_sql_script_escapes_newlines_in_error():
    """Multi-line SQL error messages must stay commented in the script."""
    from datasight.cli import _build_sql_script

    result = _make_sql_result(
        queries=[{"sql": "SELECT 1", "error": "bad thing\nDROP TABLE users;"}]
    )
    script = _build_sql_script(result, "q", "duckdb")
    for line in script.splitlines():
        stripped = line.strip()
        assert not stripped or stripped.startswith("--"), f"leaked line: {line!r}"
    assert "--   DROP TABLE users;" in script


def test_ask_print_sql_outputs_queries_to_stderr(monkeypatch, project_dir):
    async def fake_run_ask_pipeline(**kwargs):
        return _make_sql_result(
            text="here are the rows",
            queries=[{"sql": "SELECT count(*) FROM orders"}],
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["ask", "--project-dir", project_dir, "How many orders?", "--print-sql"],
    )
    assert result.exit_code == 0, result.output
    # SQL must appear on stderr so stdout stays clean for pipelines.
    assert "SQL queries executed" in result.stderr
    assert "SELECT count(*) FROM orders;" in result.stderr
    assert "SQL queries executed" not in result.stdout
    assert "SELECT count(*) FROM orders" not in result.stdout


def test_ask_print_sql_keeps_json_stdout_parseable(monkeypatch, project_dir):
    """--print-sql must not corrupt --format json output on stdout."""

    class FakeFrame:
        empty = False
        columns = ["count"]

        def __len__(self):
            return 1

        def head(self, n):  # noqa: ARG002
            return self

        def iterrows(self):
            yield 0, [10]

        def to_csv(self, index=False):  # noqa: ARG002
            return "count\n10\n"

        def to_json(self, orient="records", indent=2):  # noqa: ARG002
            return '[\n  {\n    "count": 10\n  }\n]'

    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text="",
            tool_results=[
                SimpleNamespace(
                    df=FakeFrame(),
                    plotly_spec=None,
                    meta={
                        "tool": "run_sql",
                        "sql": "SELECT count(*) FROM orders",
                        "formatted_sql": "SELECT count(*) FROM orders",
                        "error": None,
                    },
                )
            ],
            suggestions=[],
            total_input_tokens=0,
            total_output_tokens=0,
            api_calls=0,
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "How many orders?",
            "--format",
            "json",
            "--print-sql",
        ],
    )
    assert result.exit_code == 0, result.output
    # stdout must be valid JSON despite --print-sql being set.
    data = json.loads(result.stdout.strip())
    assert data == [{"count": 10}]
    # stderr carries the SQL diagnostics.
    assert "SELECT count(*) FROM orders;" in result.stderr


def test_ask_sql_script_writes_file(monkeypatch, project_dir, tmp_path):
    from datasight.cli import _question_table_prefix

    async def fake_run_ask_pipeline(**kwargs):
        return _make_sql_result(
            queries=[
                {"sql": "SELECT 1 AS a"},
                {"sql": "SELECT 2 AS b", "tool": "visualize_data"},
            ]
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    script_path = tmp_path / "out" / "queries.sql"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "Top 5 states",
            "--sql-script",
            str(script_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert script_path.exists()
    content = script_path.read_text(encoding="utf-8")
    prefix = _question_table_prefix("Top 5 states")
    assert "-- Question: Top 5 states" in content
    assert f"CREATE OR REPLACE TABLE {prefix}_1 AS" in content
    assert f"CREATE OR REPLACE TABLE {prefix}_2 AS" in content
    # The "saved to" confirmation is a diagnostic — it must land on
    # stderr so it does not corrupt machine-readable stdout.
    assert f"SQL script saved to {script_path}" in result.stderr
    assert f"SQL script saved to {script_path}" not in result.stdout


def test_ask_sql_script_keeps_json_stdout_parseable(monkeypatch, project_dir, tmp_path):
    """--sql-script must not corrupt --format json output on stdout."""

    class FakeFrame:
        empty = False
        columns = ["count"]

        def __len__(self):
            return 1

        def head(self, n):  # noqa: ARG002
            return self

        def iterrows(self):
            yield 0, [10]

        def to_csv(self, index=False):  # noqa: ARG002
            return "count\n10\n"

        def to_json(self, orient="records", indent=2):  # noqa: ARG002
            return '[\n  {\n    "count": 10\n  }\n]'

    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text="",
            tool_results=[
                SimpleNamespace(
                    df=FakeFrame(),
                    plotly_spec=None,
                    meta={
                        "tool": "run_sql",
                        "sql": "SELECT count(*) FROM orders",
                        "formatted_sql": "SELECT count(*) FROM orders",
                        "error": None,
                    },
                )
            ],
            suggestions=[],
            total_input_tokens=0,
            total_output_tokens=0,
            api_calls=0,
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    script_path = tmp_path / "queries.sql"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "How many orders?",
            "--format",
            "json",
            "--sql-script",
            str(script_path),
        ],
    )
    assert result.exit_code == 0, result.output
    # stdout must be valid JSON despite --sql-script being set.
    data = json.loads(result.stdout.strip())
    assert data == [{"count": 10}]
    # stderr carries the diagnostic confirmation.
    assert f"SQL script saved to {script_path}" in result.stderr
    # And the script file itself was actually written.
    assert script_path.exists()


def test_query_logger_tolerates_unwritable_parent(tmp_path):
    """A read-only project dir must not crash QueryLogger construction.

    Regression: ``datasight ask`` constructs a QueryLogger unconditionally,
    so a hard failure here would turn ask into a hard failure on read-only
    project directories.
    """
    import os
    import stat

    from datasight.query_log import QueryLogger

    readonly_dir = tmp_path / "readonly-project"
    readonly_dir.mkdir()
    os.chmod(readonly_dir, stat.S_IRUSR | stat.S_IXUSR)
    try:
        ql = QueryLogger(path=str(readonly_dir / ".datasight" / "query_log.jsonl"))
        # log() must also swallow the subsequent write failure.
        ql.log(
            session_id="s",
            user_question="q",
            tool="run_sql",
            sql="SELECT 1",
            execution_time_ms=1.0,
        )
    finally:
        os.chmod(readonly_dir, 0o700)


def test_ask_sql_script_rejects_with_file(project_dir, tmp_path):
    questions_path = tmp_path / "questions.txt"
    questions_path.write_text("How many orders?\n", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "--file",
            str(questions_path),
            "--sql-script",
            str(tmp_path / "out.sql"),
        ],
    )
    assert result.exit_code != 0
    assert "--sql-script cannot be combined with --file" in result.output


# ---------------------------------------------------------------------------
# Cost logging
# ---------------------------------------------------------------------------


def test_build_cost_data_known_model_returns_estimated_cost():
    from datasight.cost import build_cost_data

    data = build_cost_data(
        "claude-sonnet-4-20250514",
        api_calls=2,
        input_tokens=1_000_000,
        output_tokens=1_000_000,
    )
    assert data["api_calls"] == 2
    assert data["input_tokens"] == 1_000_000
    assert data["output_tokens"] == 1_000_000
    # 1M input @ $3 + 1M output @ $15 = $18.00
    assert data["estimated_cost"] == 18.0


def test_build_cost_data_unknown_model_returns_none_cost():
    from datasight.cost import build_cost_data

    data = build_cost_data(
        "made-up-model",
        api_calls=1,
        input_tokens=100,
        output_tokens=200,
    )
    assert data["api_calls"] == 1
    assert data["input_tokens"] == 100
    assert data["output_tokens"] == 200
    assert data["estimated_cost"] is None


def test_run_ask_pipeline_logs_cost_entry(monkeypatch, project_dir):
    """``datasight ask`` must persist a turn-level cost summary to the query log."""
    import asyncio

    from datasight import cli as cli_module
    from datasight.agent import AgentResult
    from datasight.settings import Settings

    # Stub out the LLM client and SQL runner so the pipeline does no real I/O.
    class FakeRunner:
        async def run_sql(self, sql, **kwargs):  # noqa: ARG002
            return None

    monkeypatch.setattr(cli_module, "create_llm_client", lambda **kwargs: object())
    monkeypatch.setattr(
        cli_module,
        "create_sql_runner_from_settings",
        lambda settings, project_dir: FakeRunner(),
    )

    async def fake_introspect_schema(run_sql, runner=None):  # noqa: ARG001
        return []

    monkeypatch.setattr("datasight.schema.introspect_schema", fake_introspect_schema)

    async def fake_run_agent_loop(**kwargs):
        return AgentResult(
            text="answered",
            tool_results=[],
            total_input_tokens=1500,
            total_output_tokens=400,
            api_calls=3,
        )

    monkeypatch.setattr("datasight.agent.run_agent_loop", fake_run_agent_loop)

    env_path = Path(project_dir) / ".env"
    settings = Settings.from_env(str(env_path))
    asyncio.run(
        cli_module._run_ask_pipeline(
            question="How many orders are there?",
            settings=settings,
            resolved_model="claude-sonnet-4-20250514",
            project_dir=project_dir,
            sql_dialect="duckdb",
        )
    )

    log_path = Path(project_dir) / ".datasight" / "query_log.jsonl"
    assert log_path.exists(), "query log should be created by _run_ask_pipeline"
    entries = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    cost_entries = [e for e in entries if e.get("type") == "cost"]
    assert len(cost_entries) == 1
    cost = cost_entries[0]
    assert cost["api_calls"] == 3
    assert cost["input_tokens"] == 1500
    assert cost["output_tokens"] == 400
    assert cost["user_question"] == "How many orders are there?"
    # Sonnet pricing: 1500 in @ $3/M + 400 out @ $15/M = 0.0045 + 0.006 = 0.0105
    assert cost["estimated_cost"] == 0.0105


def test_run_ask_pipeline_session_ids_unique_within_second(monkeypatch, project_dir):
    """Two CLI runs started in the same wall-clock second must get
    distinct session ids — otherwise their query-log entries become
    indistinguishable in fast batch / test scenarios.
    """
    import asyncio

    from datasight import cli as cli_module
    from datasight.agent import AgentResult
    from datasight.settings import Settings

    class FakeRunner:
        async def run_sql(self, sql, **kwargs):  # noqa: ARG002
            return None

    monkeypatch.setattr(cli_module, "create_llm_client", lambda **kwargs: object())
    monkeypatch.setattr(
        cli_module,
        "create_sql_runner_from_settings",
        lambda settings, project_dir: FakeRunner(),
    )

    async def fake_introspect_schema(run_sql, runner=None):  # noqa: ARG001
        return []

    monkeypatch.setattr("datasight.schema.introspect_schema", fake_introspect_schema)

    async def fake_run_agent_loop(**kwargs):
        return AgentResult(
            text="",
            tool_results=[],
            total_input_tokens=10,
            total_output_tokens=5,
            api_calls=1,
        )

    monkeypatch.setattr("datasight.agent.run_agent_loop", fake_run_agent_loop)

    settings = Settings.from_env(str(Path(project_dir) / ".env"))

    async def run_one(q):
        return await cli_module._run_ask_pipeline(
            question=q,
            settings=settings,
            resolved_model="claude-sonnet-4-20250514",
            project_dir=project_dir,
            sql_dialect="duckdb",
        )

    async def run_pair():
        # Back-to-back, no sleep, well within the same second.
        await run_one("first")
        await run_one("second")

    asyncio.run(run_pair())

    log_path = Path(project_dir) / ".datasight" / "query_log.jsonl"
    entries = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    cost_entries = [e for e in entries if e.get("type") == "cost"]
    assert len(cost_entries) == 2
    assert cost_entries[0]["session_id"] != cost_entries[1]["session_id"]
