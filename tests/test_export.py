"""Tests for session export."""

from datasight.export import export_dashboard_html, export_session_html


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
                "title": "Overview Section",
                "markdown": "## Overview\n\nOpening context.",
            }
        ],
        title="Section Export",
        columns=1,
    )

    assert "Section Export" in html
    assert "Overview Section" in html
    assert "section-body" in html
    assert "Overview" in html


def test_export_dashboard_section_is_full_width_row_not_card():
    """Sections must render as full-width rows, not chart-style cards."""
    html = export_dashboard_html(
        [
            {
                "id": 1,
                "type": "section",
                "title": "Banner",
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
