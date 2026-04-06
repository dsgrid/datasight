"""Tests for session export."""

from datasight.export import export_session_html


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
