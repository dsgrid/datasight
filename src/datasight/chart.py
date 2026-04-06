"""
Plotly chart renderer.

Builds a self-contained HTML page that renders a Plotly.js chart
inside an iframe, with light/dark theme support and customization controls.
"""

from __future__ import annotations

import json
from typing import Any

from datasight.templating import render_template

PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.2.min.js"


def build_chart_html(chart_dict: dict[str, Any], title: str) -> str:
    """Build a self-contained HTML page that renders the Plotly chart.

    Parameters
    ----------
    chart_dict:
        Plotly chart specification with 'data' and optional 'layout' keys.
    title:
        Chart title (used for default layout title).

    Returns
    -------
    Complete HTML string that can be rendered in an iframe.
    """
    return render_template(
        "chart",
        {
            "plotly_cdn": PLOTLY_CDN,
            "chart_json": json.dumps(chart_dict),
            "title": title,
        },
    )


# Backwards compatibility alias
_build_artifact_html = build_chart_html
