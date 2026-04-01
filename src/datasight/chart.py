"""
Plotly chart renderer.

Builds a self-contained HTML page that renders a Plotly.js chart
inside an iframe, with light/dark theme support.
"""

import json
from typing import Any

PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.2.min.js"


def _build_artifact_html(chart_dict: dict[str, Any], title: str) -> str:
    """Build a self-contained HTML page that renders the Plotly chart."""
    chart_json = json.dumps(chart_dict)
    return f"""\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<script src="{PLOTLY_CDN}"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: 'Space Grotesk', system-ui, sans-serif; background: transparent; transition: background 0.2s; }}
  #chart {{ width: 100%; height: 100vh; }}
</style>
</head>
<body>
<div id="chart"></div>
<script>
  var spec = {chart_json};
  var data = spec.data || [];
  var layout = spec.layout || {{}};

  var themes = {{
    light: {{
      paper_bgcolor: 'white',
      plot_bgcolor: 'white',
      font: {{ color: '#1a1a1a' }},
      xaxis: {{ gridcolor: '#eee', linecolor: '#ddd', zerolinecolor: '#ddd' }},
      yaxis: {{ gridcolor: '#eee', linecolor: '#ddd', zerolinecolor: '#ddd' }},
    }},
    dark: {{
      paper_bgcolor: '#161b22',
      plot_bgcolor: '#161b22',
      font: {{ color: '#e6edf3' }},
      xaxis: {{ gridcolor: '#30363d', linecolor: '#30363d', zerolinecolor: '#30363d' }},
      yaxis: {{ gridcolor: '#30363d', linecolor: '#30363d', zerolinecolor: '#30363d' }},
    }}
  }};

  function applyTheme(theme) {{
    var t = themes[theme] || themes.light;
    Plotly.relayout('chart', {{
      paper_bgcolor: t.paper_bgcolor,
      plot_bgcolor: t.plot_bgcolor,
      font: t.font,
      'xaxis.gridcolor': t.xaxis.gridcolor,
      'xaxis.linecolor': t.xaxis.linecolor,
      'xaxis.zerolinecolor': t.xaxis.zerolinecolor,
      'yaxis.gridcolor': t.yaxis.gridcolor,
      'yaxis.linecolor': t.yaxis.linecolor,
      'yaxis.zerolinecolor': t.yaxis.zerolinecolor,
    }});
    document.body.style.background = t.paper_bgcolor;
  }}

  // Listen for theme changes from parent
  window.addEventListener('message', function(e) {{
    if (e.data && e.data.type === 'theme-change') {{
      applyTheme(e.data.theme);
    }}
  }});

  // Initial render — use parent's theme if available
  var initialTheme = 'light';
  try {{ initialTheme = window.parent.document.documentElement.getAttribute('data-theme') || 'light'; }} catch(e) {{}}
  var t = themes[initialTheme] || themes.light;

  layout.autosize = true;
  layout.height = undefined;
  layout.paper_bgcolor = t.paper_bgcolor;
  layout.plot_bgcolor = t.plot_bgcolor;
  layout.font = Object.assign(layout.font || {{}}, t.font);
  layout.xaxis = Object.assign(layout.xaxis || {{}}, t.xaxis);
  layout.yaxis = Object.assign(layout.yaxis || {{}}, t.yaxis);
  document.body.style.background = t.paper_bgcolor;

  Plotly.newPlot('chart', data, layout, {{
    responsive: true,
    displayModeBar: true,
    modeBarButtonsToRemove: ['lasso2d', 'select2d'],
    displaylogo: false
  }});
</script>
</body>
</html>"""
