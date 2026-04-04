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
