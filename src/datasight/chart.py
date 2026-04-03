"""
Plotly chart renderer.

Builds a self-contained HTML page that renders a Plotly.js chart
inside an iframe, with light/dark theme support and customization controls.
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
  body {{ font-family: 'Space Grotesk', system-ui, sans-serif; background: transparent; transition: background 0.2s; display: flex; flex-direction: column; height: 100vh; }}
  #chart {{ width: 100%; flex: 1; min-height: 0; }}
  #toolbar {{
    display: flex; align-items: center; gap: 4px; padding: 4px 8px;
    border-bottom: 1px solid rgba(128,128,128,0.2); background: rgba(128,128,128,0.05);
    flex-shrink: 0;
  }}
  .chart-btn {{
    width: 28px; height: 28px; border-radius: 4px;
    border: 1px solid rgba(128,128,128,0.3); background: rgba(128,128,128,0.1);
    cursor: pointer; display: flex; align-items: center; justify-content: center;
    font-size: 14px; color: inherit; opacity: 0.6; transition: opacity 0.15s; flex-shrink: 0;
  }}
  .chart-btn:hover {{ opacity: 1; }}
  #save-btn {{ font-size: 12px; }}
  .toolbar-sep {{ flex: 1; }}
  #controls {{
    display: none; gap: 12px; align-items: center; flex-wrap: wrap; font-size: 12px;
  }}
  #controls.visible {{ display: flex; }}
  #controls label {{ color: inherit; opacity: 0.7; font-size: 11px; text-transform: uppercase; letter-spacing: 0.03em; }}
  #controls select, #controls input {{
    border: 1px solid rgba(128,128,128,0.3); border-radius: 4px;
    padding: 3px 6px; font-family: inherit; font-size: 12px;
    background: var(--ctrl-bg, transparent); color: inherit; outline: none;
  }}
  #controls select option {{ background: var(--ctrl-bg, white); color: var(--ctrl-color, #1a1a1a); }}
  #controls select:focus, #controls input:focus {{ border-color: #15a8a8; }}
  .ctrl-group {{ display: flex; align-items: center; gap: 4px; }}
</style>
</head>
<body>
<div id="toolbar">
  <button class="chart-btn" id="save-btn" onclick="saveSpec()" title="Save Plotly JSON">&#11123;</button>
  <button class="chart-btn" id="toggle-btn" onclick="toggleControls()" title="Chart controls">&#9881;</button>
  <div class="toolbar-sep"></div>
  <div id="controls">
    <div class="ctrl-group">
      <label>Type</label>
      <select id="chart-type" onchange="changeChartType(this.value)">
        <option value="bar">Bar</option>
        <option value="line">Line</option>
        <option value="scatter">Scatter</option>
        <option value="pie">Pie</option>
      </select>
    </div>
    <div class="ctrl-group">
      <label>Title</label>
      <input id="chart-title" type="text" placeholder="Chart title" oninput="updateTitle(this.value)">
    </div>
    <div class="ctrl-group">
      <label>X axis</label>
      <input id="x-label" type="text" placeholder="X axis label" oninput="updateAxisLabel('x', this.value)">
    </div>
    <div class="ctrl-group">
      <label>Y axis</label>
      <input id="y-label" type="text" placeholder="Y axis label" oninput="updateAxisLabel('y', this.value)">
    </div>
  </div>
</div>
<div id="chart"></div>
<script>
  var spec = {chart_json};
  var data = spec.data || [];
  var layout = spec.layout || {{}};

  // Store original trace data for type switching
  var origTraces = JSON.parse(JSON.stringify(data));
  var currentType = (data[0] && data[0].type) || 'bar';

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
    document.body.style.color = t.font.color;
    document.documentElement.style.colorScheme = theme;
    document.body.style.setProperty('--ctrl-bg', theme === 'dark' ? '#21262d' : 'white');
    document.body.style.setProperty('--ctrl-color', t.font.color);
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
  document.body.style.color = t.font.color;
  document.documentElement.style.colorScheme = initialTheme;
  document.body.style.setProperty('--ctrl-bg', initialTheme === 'dark' ? '#21262d' : 'white');
  document.body.style.setProperty('--ctrl-color', t.font.color);

  Plotly.newPlot('chart', data, layout, {{
    responsive: true,
    displayModeBar: true,
    modeBarButtonsToRemove: ['lasso2d', 'select2d'],
    displaylogo: false
  }});

  // Populate controls with current values
  var typeSelect = document.getElementById('chart-type');
  var mapped = currentType === 'scatter' && data[0] && data[0].mode === 'lines' ? 'line' : currentType;
  if (typeSelect.querySelector('option[value="' + mapped + '"]')) {{
    typeSelect.value = mapped;
  }}
  document.getElementById('chart-title').value = (typeof layout.title === 'string' ? layout.title : (layout.title && layout.title.text) || '');
  document.getElementById('x-label').value = (layout.xaxis && layout.xaxis.title && (typeof layout.xaxis.title === 'string' ? layout.xaxis.title : layout.xaxis.title.text)) || '';
  document.getElementById('y-label').value = (layout.yaxis && layout.yaxis.title && (typeof layout.yaxis.title === 'string' ? layout.yaxis.title : layout.yaxis.title.text)) || '';

  function toggleControls() {{
    var c = document.getElementById('controls');
    c.classList.toggle('visible');
    Plotly.Plots.resize(document.getElementById('chart'));
  }}

  function changeChartType(newType) {{
    var chartEl = document.getElementById('chart');
    var traces = origTraces;
    var indices = traces.map(function(_, i) {{ return i; }});

    if (newType === 'pie') {{
      // Switch to pie: use first trace's x as labels, y as values
      var pieData = traces.map(function(tr) {{
        return {{
          type: 'pie',
          labels: tr.x || tr.labels || [],
          values: tr.y || tr.values || [],
          name: tr.name
        }};
      }});
      Plotly.react(chartEl, pieData, Object.assign({{}}, layout, {{
        xaxis: {{ visible: false }},
        yaxis: {{ visible: false }}
      }}));
    }} else {{
      // Restore original traces with new type
      var newData = traces.map(function(tr) {{
        var d = JSON.parse(JSON.stringify(tr));
        if (newType === 'line') {{
          d.type = 'scatter';
          d.mode = 'lines';
        }} else if (newType === 'scatter') {{
          d.type = 'scatter';
          d.mode = 'markers';
        }} else {{
          d.type = newType;
          delete d.mode;
        }}
        // Map pie keys to cartesian keys if needed
        if (!d.x && d.labels) {{ d.x = d.labels; }}
        if (!d.y && d.values) {{ d.y = d.values; }}
        // Give each bar a distinct color (like pie slices)
        if (newType === 'bar' && d.x) {{
          var colors = ['#636efa','#ef553b','#00cc96','#ab63fa','#ffa15a','#19d3f3','#ff6692','#b6e880','#ff97ff','#fecb52'];
          d.marker = {{ color: d.x.map(function(_, i) {{ return colors[i % colors.length]; }}) }};
        }}
        delete d.labels;
        delete d.values;
        return d;
      }});
      var newLayout = Object.assign({{}}, layout, {{
        xaxis: Object.assign(layout.xaxis || {{}}, {{ visible: true }}),
        yaxis: Object.assign(layout.yaxis || {{}}, {{ visible: true }})
      }});
      if (newData.length === 1) newLayout.showlegend = false;
      Plotly.react(chartEl, newData, newLayout);
    }}
    currentType = newType;
  }}

  function updateTitle(val) {{
    Plotly.relayout('chart', {{ title: val }});
  }}

  function updateAxisLabel(axis, val) {{
    var update = {{}};
    update[axis + 'axis.title'] = val;
    Plotly.relayout('chart', update);
  }}

  function saveSpec() {{
    var json = JSON.stringify(spec, null, 2);
    var blob = new Blob([json], {{ type: 'application/json' }});
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url;
    var title = (layout.title && (typeof layout.title === 'string' ? layout.title : layout.title.text)) || 'plotly-spec';
    a.download = title.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').substring(0, 60) + '.json';
    a.click();
    URL.revokeObjectURL(url);
  }}
</script>
</body>
</html>"""
