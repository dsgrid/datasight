"""Tests for session export."""

import ast
import subprocess
import sys
from pathlib import Path

import duckdb

from datasight.export import (
    export_dashboard_html,
    export_session_html,
    export_session_python,
)


def _make_events():
    """Create a minimal conversation event list."""
    return [
        {"event": "user_message", "data": {"text": "How many products?"}},
        {
            "event": "tool_start",
            "data": {"tool": "run_sql", "input": {"sql": "SELECT COUNT(*) FROM products"}},
        },
        {
            "event": "tool_result",
            "data": {"type": "table", "html": "<table><tr><td>5</td></tr></table>"},
        },
        {"event": "assistant_message", "data": {"text": "There are **5** products."}},
        {"event": "user_message", "data": {"text": "Show a chart"}},
        {
            "event": "tool_result",
            "data": {"type": "chart", "html": "<html><body>chart</body></html>"},
        },
        {"event": "assistant_message", "data": {"text": "Here is the chart."}},
    ]


def test_export_basic():
    html = export_session_html(_make_events(), title="Test Export")
    assert "<!DOCTYPE html>" in html
    assert "Test Export" in html
    assert "How many products?" in html
    assert "SELECT COUNT" in html
    assert "5" in html
    assert "Show a chart" in html
    assert "iframe" in html


def test_export_rebuilds_chart_from_plotly_spec():
    events = [
        {"event": "user_message", "data": {"text": "Show generation by fuel"}},
        {
            "event": "tool_result",
            "data": {
                "type": "chart",
                "html": "",
                "title": "Generation by Fuel",
                "plotly_spec": {
                    "data": [{"type": "bar", "x": ["coal"], "y": [10]}],
                    "layout": {"title": "Generation by Fuel"},
                },
            },
        },
    ]

    html = export_session_html(events, title="Spec Chart")

    assert "Spec Chart" in html
    assert "iframe" in html
    assert "Plotly.newPlot" in html
    assert "Generation by Fuel" in html


def test_export_excludes_messages():
    events = _make_events()
    # Exclude the first Q/A block (index 0 = first user_message)
    html = export_session_html(events, exclude_indices={0})
    assert "How many products?" not in html
    # Second Q/A should still be present
    assert "Show a chart" in html


def test_export_escapes_html():
    events = [
        {"event": "user_message", "data": {"text": "<script>alert('xss')</script>"}},
        {"event": "assistant_message", "data": {"text": "Safe response."}},
    ]
    html = export_session_html(events)
    assert "<script>alert" not in html
    assert "&lt;script&gt;" in html


def test_export_empty_events():
    html = export_session_html([], title="Empty")
    assert "<!DOCTYPE html>" in html
    assert "Empty" in html


def test_export_includes_provenance():
    events = [
        {"event": "user_message", "data": {"text": "Total generation?"}},
        {"event": "assistant_message", "data": {"text": "I queried generation."}},
        {
            "event": "provenance",
            "data": {
                "model": "claude-test",
                "dialect": "duckdb",
                "project_dir": "/tmp/project",
                "llm": {
                    "api_calls": 2,
                    "input_tokens": 100,
                    "output_tokens": 40,
                    "estimated_cost": 0.01,
                },
                "warnings": [],
                "tools": [
                    {
                        "tool": "run_sql",
                        "formatted_sql": "SELECT\n  SUM(net_generation_mwh)\nFROM generation_fuel",
                        "validation": {"status": "passed", "errors": []},
                        "execution": {
                            "status": "success",
                            "execution_time_ms": 4.2,
                            "row_count": 1,
                            "column_count": 1,
                            "columns": ["sum"],
                            "error": None,
                        },
                    }
                ],
            },
        },
    ]

    html = export_session_html(events)

    assert "Run details" in html
    assert "claude-test" in html
    assert "duckdb" in html
    assert "passed" in html
    assert "SUM(net_generation_mwh)" in html


def test_export_excludes_entire_turn():
    """Excluding a turn should exclude user message, tools, AND assistant response."""
    events = [
        {"event": "user_message", "data": {"text": "Question 1"}},
        {
            "event": "tool_start",
            "data": {"tool": "run_sql", "input": {"sql": "SELECT 1"}},
        },
        {
            "event": "tool_result",
            "data": {"type": "table", "html": "<table><tr><td>1</td></tr></table>"},
        },
        {"event": "assistant_message", "data": {"text": "Answer 1"}},
        {"event": "user_message", "data": {"text": "Question 2"}},
        {"event": "assistant_message", "data": {"text": "Answer 2"}},
    ]

    # Exclude turn 0 (first Q&A)
    html = export_session_html(events, exclude_indices={0})

    # Turn 0 content should be gone
    assert "Question 1" not in html
    assert "SELECT 1" not in html
    assert "Answer 1" not in html

    # Turn 1 should still be present
    assert "Question 2" in html
    assert "Answer 2" in html


def test_export_with_intermediate_assistant_message():
    """Intermediate assistant messages shouldn't break tool association."""
    events = [
        {"event": "user_message", "data": {"text": "Complex question"}},
        {"event": "assistant_message", "data": {"text": "Let me query that..."}},
        {
            "event": "tool_start",
            "data": {"tool": "run_sql", "input": {"sql": "SELECT * FROM data"}},
        },
        {
            "event": "tool_result",
            "data": {
                "type": "table",
                "html": "<table><tr><td>query_output_value</td></tr></table>",
            },
        },
        {"event": "assistant_message", "data": {"text": "Here are the results."}},
    ]

    # Exclude turn 0 - should exclude everything
    html = export_session_html(events, exclude_indices={0})

    assert "Complex question" not in html
    assert "Let me query that" not in html
    assert "SELECT * FROM data" not in html
    assert "query_output_value" not in html
    assert "Here are the results" not in html


def test_export_multiple_turns_selective_exclusion():
    """Multiple turns with selective exclusion."""
    events = [
        {"event": "user_message", "data": {"text": "Turn 0 question"}},
        {"event": "assistant_message", "data": {"text": "Turn 0 answer"}},
        {"event": "user_message", "data": {"text": "Turn 1 question"}},
        {"event": "assistant_message", "data": {"text": "Turn 1 answer"}},
        {"event": "user_message", "data": {"text": "Turn 2 question"}},
        {"event": "assistant_message", "data": {"text": "Turn 2 answer"}},
    ]

    # Exclude turns 0 and 2, keep turn 1
    html = export_session_html(events, exclude_indices={0, 2})

    assert "Turn 0" not in html
    assert "Turn 1 question" in html
    assert "Turn 1 answer" in html
    assert "Turn 2" not in html


def test_export_dashboard_includes_notes():
    html = export_dashboard_html(
        [
            {
                "id": 1,
                "type": "note",
                "title": "Overview",
                "markdown": "## Findings\n\n- First note",
            }
        ],
        title="Dashboard Export",
        columns=1,
    )

    assert "Dashboard Export" in html
    assert "Overview" in html
    assert "data-markdown" in html
    assert "Findings" in html


def test_export_dashboard_includes_sections():
    html = export_dashboard_html(
        [
            {
                "id": 2,
                "type": "section",
                "markdown": "## Overview\n\nOpening context.",
            }
        ],
        title="Section Export",
        columns=1,
    )

    assert "Section Export" in html
    assert "section-body" in html
    assert "Overview" in html
    assert "Opening context." in html


def test_export_dashboard_section_is_full_width_row_not_card():
    """Sections must render as full-width rows, not chart-style cards."""
    html = export_dashboard_html(
        [
            {
                "id": 1,
                "type": "section",
                "markdown": "Divider text.",
            },
            {
                "id": 2,
                "type": "table",
                "title": "Numbers",
                "html": "<table><tr><td>1</td></tr></table>",
            },
        ],
        title="Mixed",
        columns=2,
    )

    section_idx = html.index('<div class="dashboard-section">')
    # The next opened div tag after the section opener is the section's children
    # (section-title or section-body) — never a dashboard-card wrapper.
    next_card_idx = html.find('<div class="dashboard-card">', section_idx)
    next_section_close = html.find("</div>\n</div>", section_idx)
    assert next_card_idx == -1 or next_card_idx > next_section_close, (
        "section is wrapped inside a dashboard-card chart tile"
    )
    assert "grid-column: 1 / -1" in html  # full-width CSS rule present


def test_export_dashboard_includes_source_metadata():
    html = export_dashboard_html(
        [
            {
                "id": 3,
                "type": "table",
                "title": "Top States",
                "html": "<table><tr><td>CO</td></tr></table>",
                "source_meta": {
                    "question": "Which states are highest?",
                    "tool": "run_sql",
                    "row_count": 12,
                    "column_count": 2,
                    "execution_time_ms": 42.4,
                    "sql": "select state from totals",
                },
            }
        ],
        title="Source Export",
        columns=1,
    )

    assert "Source Export" in html
    assert "Which states are highest?" in html
    assert "run_sql" in html
    assert "42 ms" in html
    assert "select state from totals" in html


def test_export_dashboard_omits_h1_when_title_is_empty():
    """Empty title must not render any heading and must not insert
    'datasight dashboard' or any other marketing fallback."""
    html = export_dashboard_html(
        [
            {
                "id": 1,
                "type": "table",
                "title": "Numbers",
                "html": "<table><tr><td>1</td></tr></table>",
            }
        ],
        title="",
        columns=1,
    )

    assert "<h1>" not in html
    assert "datasight dashboard" not in html
    # Browser tab still needs *something*; "Dashboard" is the neutral fallback.
    assert "<title>Dashboard</title>" in html


def test_export_dashboard_renders_user_title_when_provided():
    html = export_dashboard_html(
        [
            {
                "id": 1,
                "type": "table",
                "title": "Numbers",
                "html": "<table><tr><td>1</td></tr></table>",
            }
        ],
        title="Q3 Generation Review",
        columns=1,
    )

    assert "<h1>Q3 Generation Review</h1>" in html
    assert "<title>Q3 Generation Review</title>" in html


# ---------------------------------------------------------------------------
# export_session_python
# ---------------------------------------------------------------------------


def _python_export_events():
    """Two-turn session: a chart turn followed by a plain SQL turn."""
    chart_spec = {
        "data": [{"type": "bar", "x": ["coal", "gas", "solar"], "y": [100, 60, 40]}],
        "layout": {"title": "Generation by fuel"},
    }
    return [
        {"event": "user_message", "data": {"text": "Generation by fuel"}},
        {
            "event": "tool_start",
            "data": {
                "tool": "visualize_data",
                "input": {
                    "sql": "SELECT fuel, SUM(mwh) AS mwh FROM gen GROUP BY 1 ORDER BY 2 DESC",
                },
            },
        },
        {
            "event": "tool_result",
            "data": {
                "type": "chart",
                "title": "Generation by fuel",
                "plotly_spec": chart_spec,
            },
        },
        {
            "event": "tool_done",
            "data": {
                "sql": "SELECT fuel, SUM(mwh) AS mwh FROM gen GROUP BY 1 ORDER BY 2 DESC",
                "tool": "visualize_data",
            },
        },
        {"event": "assistant_message", "data": {"text": "Coal led."}},
        {"event": "user_message", "data": {"text": "Total MWh?"}},
        {
            "event": "tool_start",
            "data": {"tool": "run_sql", "input": {"sql": "SELECT SUM(mwh) FROM gen"}},
        },
        {
            "event": "tool_done",
            "data": {"sql": "SELECT SUM(mwh) FROM gen", "tool": "run_sql"},
        },
        {"event": "assistant_message", "data": {"text": "200 MWh."}},
    ], chart_spec


def test_export_python_emits_valid_script_with_per_turn_sql_and_chart():
    events, _ = _python_export_events()
    script = export_session_python(
        events,
        title="Smoke",
        db_path="/tmp/x.duckdb",
        db_mode="duckdb",
    )

    # Must parse as Python.
    ast.parse(script)

    # Headers and structure
    assert "Exported from datasight: Smoke" in script
    assert "import argparse" in script
    assert "import duckdb" in script
    # Defaults are editable at the top of the file; both are also overridable
    # via --db / --output-dir at runtime.
    assert "DEFAULT_DB_PATH = '/tmp/x.duckdb'" in script
    assert 'DEFAULT_OUTPUT_DIR = "."' in script
    assert '_parser.add_argument("--db"' in script
    assert '_parser.add_argument("--output-dir"' in script

    # Turn 1: SQL constant + chart spec
    assert "Turn 1: Generation by fuel" in script
    assert "SQL_1 = " in script
    assert "FROM gen GROUP BY 1 ORDER BY 2 DESC" in script
    assert "CHART_1_SPEC = json.loads" in script
    assert 'fig_1.write_html(OUTPUT_DIR / "turn_1_chart.html")' in script

    # Turn 2: plain SQL, no chart
    assert "Turn 2: Total MWh?" in script
    assert "SQL_2 = " in script
    assert "CHART_2_SPEC" not in script

    # Assistant narrative is preserved as comments
    assert "# Assistant: Coal led." in script
    assert "# Assistant: 200 MWh." in script


def test_export_python_sqlite_uses_pandas_read_sql_query():
    events, _ = _python_export_events()
    script = export_session_python(events, title="t", db_path="/tmp/x.sqlite", db_mode="sqlite")
    ast.parse(script)
    assert "import sqlite3" in script
    assert "pd.read_sql_query(sql, conn)" in script


def test_export_python_unknown_db_mode_emits_todo_scaffold():
    events, _ = _python_export_events()
    script = export_session_python(events, title="t", db_path="", db_mode="postgres")
    ast.parse(script)
    assert "Database mode: postgres" in script
    # run_sql still raises so the user knows exactly where to wire in their driver
    assert "raise NotImplementedError" in script


def test_export_python_respects_exclude_indices():
    events, _ = _python_export_events()
    script = export_session_python(
        events,
        title="t",
        db_path="/tmp/x.duckdb",
        db_mode="duckdb",
        exclude_indices={0},  # drop turn 0 (the chart turn)
    )
    ast.parse(script)
    assert "Turn 1: Generation by fuel" not in script
    assert "CHART_1_SPEC" not in script
    assert "Turn 2: Total MWh?" in script


def test_export_python_runs_against_real_duckdb(tmp_path):
    """End-to-end: generated script imports cleanly, queries a real .duckdb,
    and writes the chart HTML into the cwd by default (output-dir defaults to '.')."""
    db_path = tmp_path / "session.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE gen AS SELECT * FROM (VALUES "
        "('coal', 100), ('gas', 60), ('solar', 40)) t(fuel, mwh)"
    )
    conn.close()

    events, _ = _python_export_events()
    script = export_session_python(events, title="Live", db_path=str(db_path), db_mode="duckdb")

    script_path = tmp_path / "session.py"
    script_path.write_text(script)

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
    )
    assert result.returncode == 0, result.stderr
    assert "[turn 1] 3 rows" in result.stdout
    assert "[turn 2] 1 rows" in result.stdout
    assert (tmp_path / "turn_1_chart.html").exists()


def test_export_python_argparse_overrides_db_and_output_dir(tmp_path):
    """--db and --output-dir override the hardcoded defaults at runtime."""
    real_db = tmp_path / "real.duckdb"
    duckdb.connect(str(real_db)).execute(
        "CREATE TABLE gen AS SELECT * FROM (VALUES "
        "('coal', 100), ('gas', 60), ('solar', 40)) t(fuel, mwh)"
    ).close()

    events, _ = _python_export_events()
    # The hardcoded default points somewhere that does NOT exist; --db must win.
    script = export_session_python(
        events,
        title="Override",
        db_path="/nonexistent/wrong.duckdb",
        db_mode="duckdb",
    )
    script_path = tmp_path / "session.py"
    script_path.write_text(script)

    charts_dir = tmp_path / "charts"
    result = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--db",
            str(real_db),
            "--output-dir",
            str(charts_dir),
        ],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
    )
    assert result.returncode == 0, result.stderr
    assert (charts_dir / "turn_1_chart.html").exists()
    # The default-cwd location must NOT be written when --output-dir is set.
    assert not (tmp_path / "turn_1_chart.html").exists()


def test_export_python_help_lists_db_and_output_dir(tmp_path):
    """`python script.py --help` documents both flags with their defaults."""
    events, _ = _python_export_events()
    script = export_session_python(
        events,
        title="t",
        db_path="/some/where.duckdb",
        db_mode="duckdb",
    )
    script_path = tmp_path / "s.py"
    script_path.write_text(script)
    result = subprocess.run(
        [sys.executable, str(script_path), "--help"],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
    )
    assert result.returncode == 0, result.stderr
    assert "--db" in result.stdout
    assert "--output-dir" in result.stdout
    assert "/some/where.duckdb" in result.stdout


def test_cli_export_format_py_writes_runnable_script(tmp_path, monkeypatch):
    """CLI: `datasight export <id> --format py` writes a parseable script
    whose DB_PATH points at the project's database."""
    import json
    import re

    from click.testing import CliRunner

    from datasight.cli import cli

    # Don't let the host shell's DATASIGHT_* / DB_PATH leak into Settings.from_env.
    for key in ("DB_PATH", "DB_MODE", "DATASIGHT_PROJECT"):
        monkeypatch.delenv(key, raising=False)

    project_dir = tmp_path / "proj"
    (project_dir / ".datasight" / "conversations").mkdir(parents=True)
    db_path = project_dir / "data.duckdb"
    duckdb.connect(str(db_path)).close()
    # The settings loader reads .env from the project directory.
    (project_dir / ".env").write_text(f"DB_MODE=duckdb\nDB_PATH={db_path}\n")

    events, _ = _python_export_events()
    (project_dir / ".datasight" / "conversations" / "abc123.json").write_text(
        json.dumps({"title": "Live", "messages": [], "events": events})
    )

    out_path = tmp_path / "session.py"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "export",
            "abc123",
            "--format",
            "py",
            "--project-dir",
            str(project_dir),
            "--output",
            str(out_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert out_path.exists()
    text = out_path.read_text()
    ast.parse(text)
    assert "import duckdb" in text
    # Compare by realpath so /private/var vs /var on macOS doesn't trip the test.
    match = re.search(r"DEFAULT_DB_PATH\s*=\s*'([^']+)'", text)
    assert match, "DEFAULT_DB_PATH constant not found in script"
    assert Path(match.group(1)).resolve() == db_path.resolve()
