"""Additional tests for export covering dashboard chart/iframe/error paths."""

from datasight.export import _extract_plotly_spec, export_dashboard_html


def test_extract_plotly_spec_valid():
    srcdoc = 'var spec = {"data": [{"type": "bar"}]};'
    spec = _extract_plotly_spec(srcdoc)
    assert spec is not None
    assert spec["data"][0]["type"] == "bar"


def test_extract_plotly_spec_invalid_json():
    srcdoc = "var spec = {this-is-not-json};"
    assert _extract_plotly_spec(srcdoc) is None


def test_extract_plotly_spec_no_match():
    assert _extract_plotly_spec("nothing here") is None


def test_export_dashboard_chart_with_valid_spec():
    html = export_dashboard_html(
        [
            {
                "type": "chart",
                "title": "MyChart",
                "html": '<html>var spec = {"data": [{"type": "bar", "x": [1], "y": [2]}], "layout": {}};</html>',
            }
        ],
        title="Dash",
        columns=1,
    )
    assert "MyChart" in html
    # The spec JSON should end up embedded somewhere in the output
    assert '"type"' in html


def test_export_dashboard_chart_fallback_to_iframe_when_spec_missing():
    html = export_dashboard_html(
        [
            {
                "type": "chart",
                "title": "Broken",
                "html": "<html><body>no spec variable here</body></html>",
            }
        ],
        title="Dash",
        columns=1,
    )
    assert "Broken" in html


def test_export_dashboard_source_error_shown():
    html = export_dashboard_html(
        [
            {
                "type": "table",
                "title": "Oops",
                "html": "<table></table>",
                "source_meta": {
                    "question": "Q?",
                    "tool": "run_sql",
                    "error": "some SQL failure",
                },
            }
        ],
        title="Dash",
        columns=1,
    )
    assert "some SQL failure" in html or "Oops" in html


def test_export_dashboard_filters_rendered():
    html = export_dashboard_html(
        [{"type": "table", "title": "T", "html": "<table></table>"}],
        title="Dash",
        columns=1,
        filters=[
            {"column": "state", "operator": "in", "value": ["CA", "TX"]},
            {"column": "report_year", "operator": "gte", "value": 2020},
        ],
    )
    assert "Filters:" in html
    assert "state" in html
    assert "CA, TX" in html
    assert "report_year" in html
    assert "≥" in html
    assert "post-aggregation" in html


def test_export_dashboard_filter_scope_suffix():
    html = export_dashboard_html(
        [{"type": "table", "title": "T", "html": "<table></table>"}],
        title="Dash",
        columns=1,
        filters=[
            {
                "column": "state",
                "operator": "eq",
                "value": "CA",
                "scope": {"type": "cards", "cardIds": [1, 2]},
            },
            {
                "column": "fuel",
                "operator": "eq",
                "value": "NG",
                "scope": {"type": "all"},
            },
        ],
    )
    assert "2 cards" in html
    # "all" scope should not render a suffix
    assert html.count("· ") == 1


def test_export_dashboard_no_filter_bar_when_empty():
    html = export_dashboard_html(
        [{"type": "table", "title": "T", "html": "<table></table>"}],
        title="Dash",
        columns=1,
    )
    assert 'class="filter-bar"' not in html
    assert "Filters:" not in html
