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
