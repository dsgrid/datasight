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


def test_export_dashboard_chart_uses_render_plotly_spec():
    # The live UI streams chart tool_results with html="" and renders the
    # data-bound spec from render_plotly_spec. The export must use that
    # rendered spec rather than try to regex it back out of an empty html.
    spec = {"data": [{"type": "scatter", "x": [0], "y": [9]}], "layout": {}}
    html = export_dashboard_html(
        [
            {
                "type": "chart",
                "title": "FromRenderSpec",
                "html": "",
                "render_plotly_spec": spec,
            }
        ],
        title="Dash",
        columns=1,
    )
    assert "FromRenderSpec" in html
    assert '"y": [9]' in html or '"y":[9]' in html
    assert 'srcdoc=""' not in html


def test_export_dashboard_chart_prefers_render_over_unbound_plotly_spec():
    # plotly_spec is an unbound template referencing column names; only
    # render_plotly_spec has actual data arrays. We must pick the rendered one.
    unbound = {"data": [{"type": "box", "x": "col_x", "y": "col_y"}], "layout": {}}
    rendered = {"data": [{"type": "box", "x": [1, 2, 3], "y": [4, 5, 6]}], "layout": {}}
    html = export_dashboard_html(
        [
            {
                "type": "chart",
                "title": "Bound",
                "html": "",
                "plotly_spec": unbound,
                "render_plotly_spec": rendered,
            }
        ],
        title="Dash",
        columns=1,
    )
    # Concrete values present, the column-name template is not.
    assert "[1, 2, 3]" in html
    assert '"col_x"' not in html


def test_export_dashboard_chart_falls_back_to_plotly_spec_when_no_render():
    # When only plotly_spec is provided (e.g. spec already has data inline),
    # we should still use it.
    spec = {"data": [{"type": "bar", "x": [1], "y": [2]}], "layout": {}}
    html = export_dashboard_html(
        [
            {
                "type": "chart",
                "title": "FromPlotlySpec",
                "html": "",
                "plotly_spec": spec,
            }
        ],
        title="Dash",
        columns=1,
    )
    assert "FromPlotlySpec" in html
    assert '"y": [2]' in html or '"y":[2]' in html


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
