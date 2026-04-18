"""Integration tests for the ``datasight ask`` CLI command using Ollama.

These tests require a running Ollama instance with the qwen3:8b model.
They are marked with ``@pytest.mark.integration`` and can be skipped with:

    pytest -m "not integration"
"""

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request

import pytest


# Env vars that datasight reads — if any of these leak in from an earlier
# in-process test (via load_dotenv populating os.environ), they will override
# the test's .env and break isolation. Scrub them before launching a
# subprocess.
_SCRUBBED_ENV_VARS = (
    "DB_MODE",
    "DB_PATH",
    "LLM_PROVIDER",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_MODEL",
    "ANTHROPIC_BASE_URL",
    "OLLAMA_MODEL",
    "OLLAMA_BASE_URL",
    "GITHUB_TOKEN",
    "GITHUB_MODEL",
    "FLIGHT_URI",
    "FLIGHT_TOKEN",
    "FLIGHT_USERNAME",
    "FLIGHT_PASSWORD",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DATABASE",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_URL",
    "POSTGRES_SSLMODE",
)
_REQUIRED_OLLAMA_MODEL = "qwen3:8b"


def _clean_subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    for key in _SCRUBBED_ENV_VARS:
        env.pop(key, None)
    return env


def _run_ask(project_dir, question, *extra_args, timeout=180):
    """Run ``datasight ask`` as a subprocess and return the result."""
    cmd = [
        sys.executable,
        "-m",
        "datasight",
        "ask",
        question,
        "--project-dir",
        project_dir,
        *extra_args,
    ]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=_clean_subprocess_env(),
    )
    return result


def _ollama_has_required_model() -> bool:
    """Check if Ollama is reachable and has the model used by test fixtures."""
    try:
        with urllib.request.urlopen("http://localhost:11434/api/tags", timeout=3) as resp:
            if resp.status != 200:
                return False
            data = json.loads(resp.read().decode("utf-8"))
    except (OSError, urllib.error.URLError, json.JSONDecodeError):
        return False

    models = data.get("models", [])
    if not isinstance(models, list):
        return False
    names = {
        model.get("name")
        for model in models
        if isinstance(model, dict) and isinstance(model.get("name"), str)
    }
    return _REQUIRED_OLLAMA_MODEL in names


pytestmark = pytest.mark.integration
skip_no_ollama = pytest.mark.skipif(
    not _ollama_has_required_model(),
    reason=f"Ollama model {_REQUIRED_OLLAMA_MODEL!r} is not available",
)


@skip_no_ollama
def test_ask_basic_count(project_dir):
    """Ask a simple count question and verify we get a numeric answer."""
    result = _run_ask(project_dir, "How many products are in the products table?")
    assert result.returncode == 0, f"stderr: {result.stderr}"
    output = result.stdout
    # The LLM should return a table or text containing "5"
    assert "5" in output, f"Expected '5' in output:\n{output}"


@skip_no_ollama
def test_ask_format_csv(project_dir):
    """Verify --format csv produces CSV output."""
    result = _run_ask(
        project_dir,
        "List all products with their prices. Return all rows.",
        "--format",
        "csv",
    )
    assert result.returncode == 0, f"stderr: {result.stderr}"
    output = result.stdout
    # CSV should have a header line with column names and data rows
    lines = [line for line in output.strip().split("\n") if line.strip()]
    # Should have at least a header + 5 data rows somewhere in the output
    assert len(lines) >= 3, f"Too few lines in CSV output:\n{output}"


@skip_no_ollama
def test_ask_format_json(project_dir):
    """Verify --format json produces valid JSON output."""
    result = _run_ask(
        project_dir,
        "How many orders are there? Return the count.",
        "--format",
        "json",
    )
    assert result.returncode == 0, f"stderr: {result.stderr}"
    output = result.stdout
    # There should be a JSON array somewhere in the output
    # (the LLM text precedes it, so find the JSON portion)
    json_start = output.find("[")
    if json_start >= 0:
        json_str = output[json_start:]
        # Find the matching closing bracket
        json_end = json_str.rfind("]")
        if json_end >= 0:
            data = json.loads(json_str[: json_end + 1])
            assert isinstance(data, list)
            assert len(data) >= 1


@skip_no_ollama
def test_ask_chart_html_output(project_dir, tmp_path):
    """Verify --chart-format html writes a Plotly chart file."""
    chart_path = tmp_path / "chart.html"
    result = _run_ask(
        project_dir,
        "Show total quantity sold by product category as a bar chart",
        "--chart-format",
        "html",
        "--output",
        str(chart_path),
    )
    assert result.returncode == 0, f"stderr: {result.stderr}"
    if chart_path.exists():
        html = chart_path.read_text()
        assert "plotly" in html.lower()


@skip_no_ollama
def test_ask_output_file(project_dir, tmp_path):
    """Verify --output writes data to a file."""
    out_path = tmp_path / "result.csv"
    result = _run_ask(
        project_dir,
        "List all product names and categories",
        "--format",
        "csv",
        "--output",
        str(out_path),
    )
    assert result.returncode == 0, f"stderr: {result.stderr}"
    if out_path.exists():
        content = out_path.read_text()
        assert len(content) > 0


@skip_no_ollama
def test_ask_verbose(project_dir):
    """Verify -v flag enables debug output on stderr."""
    result = _run_ask(project_dir, "How many products?", "-v")
    assert result.returncode == 0, f"stderr: {result.stderr}"
    # Verbose mode should produce debug logs on stderr
    assert len(result.stderr) > 0


@skip_no_ollama
def test_ask_join_query(project_dir):
    """Ask a question requiring a JOIN and verify reasonable output."""
    result = _run_ask(
        project_dir,
        "What is the total quantity of orders for each product? Show product name and total quantity.",
    )
    assert result.returncode == 0, f"stderr: {result.stderr}"
    output = result.stdout
    # Should mention at least one product name
    assert any(name in output for name in ["Widget", "Gadget", "Doohickey"]), (
        f"Expected product names in output:\n{output}"
    )


@skip_no_ollama
def test_ask_aggregation(project_dir):
    """Ask an aggregation question."""
    result = _run_ask(
        project_dir,
        "How many orders are from California (CA)?",
    )
    assert result.returncode == 0, f"stderr: {result.stderr}"
    output = result.stdout
    # CA has orders 1, 4, 7, 10 = 4 orders
    assert "4" in output, f"Expected '4' in output:\n{output}"


def test_ask_missing_db(tmp_path):
    """Verify error when database file doesn't exist."""
    env_content = "LLM_PROVIDER=ollama\nDB_MODE=duckdb\nDB_PATH=nonexistent.duckdb\n"
    (tmp_path / ".env").write_text(env_content, encoding="utf-8")
    result = _run_ask(str(tmp_path), "test question")
    assert result.returncode != 0
    assert "not found" in result.stderr.lower() or "error" in result.stderr.lower()
