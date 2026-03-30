"""
Plotly chart generator with interactive chart-type switching.

Generates Plotly figures with multiple trace sets and a button bar to
switch between chart types (bar, line, pie, scatter, etc.). Charts are
rendered inside an iframe for full Plotly interactivity.
"""

import json
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.2.min.js"

THEME = {
    "navy": "#023d60",
    "cream": "#e7e1cf",
    "teal": "#15a8a8",
    "orange": "#fe5d26",
    "magenta": "#bf1363",
}
PALETTE = [THEME["teal"], THEME["orange"], THEME["magenta"], THEME["navy"]]


class InteractiveChartGenerator:
    """Builds Plotly figures with updatemenus for chart-type switching."""

    def _apply_standard_layout(self, fig: go.Figure) -> go.Figure:
        fig.update_layout(
            font=dict(family="Space Grotesk, system-ui, sans-serif"),
            paper_bgcolor="white",
            plot_bgcolor="white",
            xaxis=dict(gridcolor="#eee"),
            yaxis=dict(gridcolor="#eee"),
        )
        if fig.layout.updatemenus:
            fig.update_layout(margin=dict(t=80))
        return fig

    def generate_chart_from_spec(
        self,
        df: pd.DataFrame,
        chart_type: str,
        x: str | None = None,
        y: str | None = None,
        color: str | None = None,
        title: str = "Chart",
    ) -> dict[str, Any]:
        """Build a chart from explicit LLM-specified parameters."""
        if df.empty:
            raise ValueError("Cannot visualize empty DataFrame")

        df = _coerce_dates(df)

        # Infer x/y from columns if not provided
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        non_numeric_cols = [c for c in df.columns if c not in numeric_cols]

        if x is None:
            x = non_numeric_cols[0] if non_numeric_cols else df.columns[0]
        if y is None and chart_type not in ("histogram", "pie"):
            y = numeric_cols[0] if numeric_cols else df.columns[1] if len(df.columns) > 1 else df.columns[0]

        fig = go.Figure()

        if chart_type == "bar":
            if color and color in df.columns:
                for i, val in enumerate(df[color].unique()):
                    sub = df[df[color] == val]
                    fig.add_trace(go.Bar(
                        x=sub[x], y=sub[y], name=str(val),
                        marker_color=PALETTE[i % len(PALETTE)],
                    ))
                fig.update_layout(barmode="group")
            else:
                fig.add_trace(go.Bar(x=df[x], y=df[y], marker_color=THEME["teal"]))

        elif chart_type == "horizontal_bar":
            if color and color in df.columns:
                for i, val in enumerate(df[color].unique()):
                    sub = df[df[color] == val]
                    fig.add_trace(go.Bar(
                        y=sub[x], x=sub[y], name=str(val),
                        orientation="h", marker_color=PALETTE[i % len(PALETTE)],
                    ))
                fig.update_layout(barmode="group")
            else:
                fig.add_trace(go.Bar(y=df[x], x=df[y], orientation="h", marker_color=THEME["orange"]))

        elif chart_type == "line":
            if color and color in df.columns:
                for i, val in enumerate(df[color].unique()):
                    sub = df[df[color] == val].sort_values(x)
                    fig.add_trace(go.Scatter(
                        x=sub[x], y=sub[y], mode="lines+markers", name=str(val),
                        line=dict(color=PALETTE[i % len(PALETTE)]),
                    ))
            else:
                sorted_df = df.sort_values(x)
                fig.add_trace(go.Scatter(
                    x=sorted_df[x], y=sorted_df[y], mode="lines+markers",
                    line=dict(color=THEME["teal"]),
                ))

        elif chart_type == "scatter":
            if color and color in df.columns:
                for i, val in enumerate(df[color].unique()):
                    sub = df[df[color] == val]
                    fig.add_trace(go.Scatter(
                        x=sub[x], y=sub[y], mode="markers", name=str(val),
                        marker=dict(color=PALETTE[i % len(PALETTE)], size=8),
                    ))
            else:
                fig.add_trace(go.Scatter(
                    x=df[x], y=df[y], mode="markers",
                    marker=dict(color=THEME["magenta"], size=8),
                ))

        elif chart_type == "pie":
            labels_col = x
            values_col = y if y else numeric_cols[0] if numeric_cols else df.columns[1]
            fig.add_trace(go.Pie(
                labels=df[labels_col], values=df[values_col],
                marker=dict(colors=_cycle(PALETTE, len(df))),
            ))

        elif chart_type == "area":
            if color and color in df.columns:
                for i, val in enumerate(df[color].unique()):
                    sub = df[df[color] == val].sort_values(x)
                    fig.add_trace(go.Scatter(
                        x=sub[x], y=sub[y], mode="lines", name=str(val),
                        line=dict(color=PALETTE[i % len(PALETTE)]),
                        fill="tozeroy" if i == 0 else "tonexty",
                        stackgroup="one",
                    ))
            else:
                sorted_df = df.sort_values(x)
                fig.add_trace(go.Scatter(
                    x=sorted_df[x], y=sorted_df[y], mode="lines",
                    line=dict(color=THEME["teal"]), fill="tozeroy",
                ))

        elif chart_type == "histogram":
            col = x
            fig.add_trace(go.Histogram(x=df[col], marker_color=THEME["teal"]))

        elif chart_type == "box":
            if color and color in df.columns:
                for i, val in enumerate(df[color].unique()):
                    sub = df[df[color] == val]
                    fig.add_trace(go.Box(
                        y=sub[y] if y else sub[x], name=str(val),
                        marker_color=PALETTE[i % len(PALETTE)],
                    ))
            else:
                col = y if y else x
                fig.add_trace(go.Box(y=df[col], marker_color=THEME["teal"]))

        elif chart_type == "heatmap":
            # Expects pivoted data or numeric matrix
            if x and y and color:
                pivoted = df.pivot_table(index=y, columns=x, values=color, aggfunc="sum").fillna(0)
                fig.add_trace(go.Heatmap(
                    z=pivoted.values, x=pivoted.columns.tolist(), y=pivoted.index.tolist(),
                    colorscale=[[0, THEME["navy"]], [0.5, THEME["cream"]], [1, THEME["teal"]]],
                ))
            else:
                num_df = df.select_dtypes(include=["number"])
                corr = num_df.corr()
                fig.add_trace(go.Heatmap(
                    z=corr.values, x=corr.columns.tolist(), y=corr.index.tolist(),
                    colorscale=[[0, THEME["navy"]], [0.5, THEME["cream"]], [1, THEME["teal"]]],
                    zmin=-1, zmax=1,
                ))
        else:
            # Fallback to heuristic
            return self.generate_chart(df, title)

        fig.update_layout(
            title=title,
            xaxis_title=x if chart_type not in ("pie", "histogram") else None,
            yaxis_title=y if chart_type not in ("pie", "histogram", "horizontal_bar") else None,
            hovermode="closest",
        )
        self._apply_standard_layout(fig)
        return json.loads(pio.to_json(fig))

    def generate_chart(self, df: pd.DataFrame, title: str = "Chart") -> dict[str, Any]:
        if df.empty:
            raise ValueError("Cannot visualize empty DataFrame")

        df = _coerce_dates(df)

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        categorical_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()
        datetime_cols = df.select_dtypes(include=["datetime64"]).columns.tolist()

        if datetime_cols and categorical_cols and len(numeric_cols) == 1:
            # Long-format time series: pivot categorical into separate lines
            fig = self._pivoted_timeseries(
                df,
                datetime_cols[0],
                categorical_cols[0],
                numeric_cols[0],
                title,
            )
        elif datetime_cols and numeric_cols:
            fig = self._multi_timeseries(df, datetime_cols[0], numeric_cols, title)
        elif len(categorical_cols) == 1 and len(numeric_cols) >= 1:
            fig = self._multi_cat_num(df, categorical_cols[0], numeric_cols[0], title)
        elif categorical_cols and len(numeric_cols) == 1:
            fig = self._multi_cat_num(df, categorical_cols[0], numeric_cols[0], title)
        elif len(numeric_cols) == 2 and not categorical_cols:
            fig = self._multi_two_numeric(df, numeric_cols[0], numeric_cols[1], title)
        elif len(numeric_cols) == 1 and not categorical_cols:
            fig = self._multi_single_numeric(df, numeric_cols[0], title)
        elif len(numeric_cols) >= 3:
            fig = self._multi_many_numeric(df, numeric_cols, title)
        elif len(categorical_cols) >= 2 and not numeric_cols:
            fig = self._grouped_bar(df, categorical_cols, title)
        elif len(df.columns) >= 2:
            fig = self._generic_chart(df, df.columns[0], df.columns[1], title)
        else:
            fig = self._table_figure(df, title)

        return json.loads(pio.to_json(fig))

    # -- multi-view builders ------------------------------------------------

    def _pivoted_timeseries(self, df, time_col, cat_col, val_col, title):
        """Pivot long-format data (date, category, value) into multi-line chart."""
        pivoted = df.pivot_table(
            index=time_col,
            columns=cat_col,
            values=val_col,
            aggfunc="sum",
        ).reset_index()
        pivoted = pivoted.sort_values(time_col)

        value_cols = [c for c in pivoted.columns if c != time_col][:10]
        s = len(value_cols)
        fig = go.Figure()

        for kind in ("line", "bar", "area", "scatter"):
            for i, col in enumerate(value_cols):
                c = PALETTE[i % len(PALETTE)]
                vis = kind == "line"
                name = str(col)
                if kind == "line":
                    fig.add_trace(
                        go.Scatter(
                            x=pivoted[time_col],
                            y=pivoted[col],
                            mode="lines+markers",
                            name=name,
                            line=dict(color=c),
                            visible=vis,
                        )
                    )
                elif kind == "bar":
                    fig.add_trace(
                        go.Bar(
                            x=pivoted[time_col],
                            y=pivoted[col],
                            name=name,
                            marker_color=c,
                            visible=vis,
                        )
                    )
                elif kind == "area":
                    fig.add_trace(
                        go.Scatter(
                            x=pivoted[time_col],
                            y=pivoted[col],
                            mode="lines",
                            name=name,
                            line=dict(color=c),
                            fill="tozeroy" if i == 0 else "tonexty",
                            visible=vis,
                        )
                    )
                else:
                    fig.add_trace(
                        go.Scatter(
                            x=pivoted[time_col],
                            y=pivoted[col],
                            mode="markers",
                            name=name,
                            marker=dict(color=c, size=8),
                            visible=vis,
                        )
                    )

        fig.update_layout(
            title=title,
            xaxis_title=time_col,
            yaxis_title=val_col,
            hovermode="x unified",
            updatemenus=[
                _menu(
                    [
                        _btn("Line", [True] * s + [False] * s * 3),
                        _btn("Bar", [False] * s + [True] * s + [False] * s * 2),
                        _btn("Area", [False] * s * 2 + [True] * s + [False] * s),
                        _btn("Scatter", [False] * s * 3 + [True] * s),
                    ]
                )
            ],
        )
        self._apply_standard_layout(fig)
        return fig

    def _multi_cat_num(self, df, cat_col, num_col, title):
        agg = df.groupby(cat_col, sort=False)[num_col].sum().reset_index()
        cats, vals = agg[cat_col].tolist(), agg[num_col].tolist()

        fig = go.Figure()
        fig.add_trace(go.Bar(x=cats, y=vals, marker_color=THEME["teal"], visible=True))
        fig.add_trace(
            go.Bar(y=cats, x=vals, marker_color=THEME["orange"], orientation="h", visible=False)
        )
        fig.add_trace(
            go.Scatter(
                x=cats,
                y=vals,
                mode="lines+markers",
                line=dict(color=THEME["magenta"]),
                visible=False,
            )
        )
        fig.add_trace(
            go.Pie(
                labels=cats,
                values=vals,
                marker=dict(colors=_cycle(PALETTE, len(cats))),
                visible=False,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=cats,
                y=vals,
                mode="markers",
                marker=dict(color=THEME["navy"], size=10),
                visible=False,
            )
        )

        n = 5
        fig.update_layout(
            title=title,
            xaxis_title=cat_col,
            yaxis_title=num_col,
            updatemenus=[
                _menu(
                    [
                        _btn("Bar", [True] + [False] * (n - 1)),
                        _btn("Horiz Bar", [False, True] + [False] * (n - 2)),
                        _btn("Line", [False, False, True] + [False] * (n - 3)),
                        _btn("Pie", [False, False, False, True, False]),
                        _btn("Scatter", [False] * 4 + [True]),
                    ]
                )
            ],
        )
        self._apply_standard_layout(fig)
        return fig

    def _multi_timeseries(self, df, time_col, value_cols, title):
        value_cols = value_cols[:5]
        s = len(value_cols)
        fig = go.Figure()

        for kind in ("line", "bar", "area", "scatter"):
            for i, col in enumerate(value_cols):
                c = PALETTE[i % len(PALETTE)]
                vis = kind == "line"
                if kind == "line":
                    fig.add_trace(
                        go.Scatter(
                            x=df[time_col],
                            y=df[col],
                            mode="lines+markers",
                            name=col,
                            line=dict(color=c),
                            visible=vis,
                        )
                    )
                elif kind == "bar":
                    fig.add_trace(
                        go.Bar(x=df[time_col], y=df[col], name=col, marker_color=c, visible=vis)
                    )
                elif kind == "area":
                    fig.add_trace(
                        go.Scatter(
                            x=df[time_col],
                            y=df[col],
                            mode="lines",
                            name=col,
                            line=dict(color=c),
                            fill="tozeroy" if i == 0 else "tonexty",
                            visible=vis,
                        )
                    )
                else:
                    fig.add_trace(
                        go.Scatter(
                            x=df[time_col],
                            y=df[col],
                            mode="markers",
                            name=col,
                            marker=dict(color=c, size=8),
                            visible=vis,
                        )
                    )

        fig.update_layout(
            title=title,
            xaxis_title=time_col,
            yaxis_title="Value" if s > 1 else value_cols[0],
            hovermode="x unified",
            updatemenus=[
                _menu(
                    [
                        _btn("Line", [True] * s + [False] * s * 3),
                        _btn("Bar", [False] * s + [True] * s + [False] * s * 2),
                        _btn("Area", [False] * s * 2 + [True] * s + [False] * s),
                        _btn("Scatter", [False] * s * 3 + [True] * s),
                    ]
                )
            ],
        )
        self._apply_standard_layout(fig)
        return fig

    def _multi_two_numeric(self, df, x_col, y_col, title):
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df[x_col],
                y=df[y_col],
                mode="markers",
                marker=dict(color=THEME["magenta"], size=8),
                visible=True,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=df[x_col],
                y=df[y_col],
                mode="lines+markers",
                line=dict(color=THEME["teal"]),
                visible=False,
            )
        )
        fig.add_trace(
            go.Bar(x=df[x_col], y=df[y_col], marker_color=THEME["orange"], visible=False)
        )

        fig.update_layout(
            title=title,
            xaxis_title=x_col,
            yaxis_title=y_col,
            updatemenus=[
                _menu(
                    [
                        _btn("Scatter", [True, False, False]),
                        _btn("Line", [False, True, False]),
                        _btn("Bar", [False, False, True]),
                    ]
                )
            ],
        )
        self._apply_standard_layout(fig)
        return fig

    def _multi_single_numeric(self, df, col, title):
        fig = go.Figure()
        fig.add_trace(go.Histogram(x=df[col], marker_color=THEME["teal"], visible=True))
        fig.add_trace(go.Box(y=df[col], marker_color=THEME["orange"], visible=False))
        fig.add_trace(
            go.Violin(
                y=df[col], fillcolor=THEME["magenta"], line_color=THEME["navy"], visible=False
            )
        )

        fig.update_layout(
            title=title,
            xaxis_title=col,
            yaxis_title="Count",
            updatemenus=[
                _menu(
                    [
                        _btn("Histogram", [True, False, False]),
                        _btn("Box Plot", [False, True, False]),
                        _btn("Violin", [False, False, True]),
                    ]
                )
            ],
        )
        self._apply_standard_layout(fig)
        return fig

    def _multi_many_numeric(self, df, numeric_cols, title):
        cols = numeric_cols[:5]
        corr = df[cols].corr()
        cscale = [[0.0, THEME["navy"]], [0.5, THEME["cream"]], [1.0, THEME["teal"]]]

        fig = go.Figure()
        fig.add_trace(
            go.Heatmap(
                z=corr.values, x=cols, y=cols, colorscale=cscale, zmin=-1, zmax=1, visible=True
            )
        )
        fig.add_trace(
            go.Bar(x=cols, y=df[cols].mean().values, marker_color=THEME["orange"], visible=False)
        )
        for i, c in enumerate(cols):
            fig.add_trace(
                go.Box(y=df[c], name=c, marker_color=PALETTE[i % len(PALETTE)], visible=False)
            )

        nb = len(cols)
        fig.update_layout(
            title=title,
            updatemenus=[
                _menu(
                    [
                        _btn("Correlation", [True, False] + [False] * nb),
                        _btn("Means", [False, True] + [False] * nb),
                        _btn("Box Plots", [False, False] + [True] * nb),
                    ]
                )
            ],
        )
        self._apply_standard_layout(fig)
        return fig

    def _grouped_bar(self, df: pd.DataFrame, cat_cols: list[str], title: str) -> go.Figure:
        col_a, col_b = cat_cols[0], cat_cols[1]
        counts = df.groupby([col_a, col_b]).size().to_frame("count").reset_index()
        fig = go.Figure()
        for i, val in enumerate(counts[col_b].unique()):
            sub = counts[counts[col_b] == val]
            fig.add_trace(
                go.Bar(
                    x=sub[col_a],
                    y=sub["count"],
                    name=str(val),
                    marker_color=PALETTE[i % len(PALETTE)],
                )
            )
        fig.update_layout(title=title, barmode="group", xaxis_title=col_a, yaxis_title="Count")
        self._apply_standard_layout(fig)
        return fig

    def _generic_chart(
        self, df: pd.DataFrame, x_col: str, y_col: str, title: str
    ) -> go.Figure:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=df[x_col], y=df[y_col], marker_color=THEME["teal"]))
        fig.update_layout(title=title, xaxis_title=str(x_col), yaxis_title=str(y_col))
        self._apply_standard_layout(fig)
        return fig

    def _table_figure(self, df: pd.DataFrame, title: str) -> go.Figure:
        fig = go.Figure(
            data=[
                go.Table(
                    header=dict(
                        values=list(df.columns),
                        fill_color=THEME["navy"],
                        font=dict(color="white"),
                    ),
                    cells=dict(
                        values=[df[c].tolist() for c in df.columns],
                        fill_color="white",
                    ),
                )
            ]
        )
        fig.update_layout(title=title)
        self._apply_standard_layout(fig)
        return fig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _menu(buttons: list[dict[str, Any]]) -> dict[str, Any]:
    return dict(
        type="buttons",
        direction="right",
        x=0.0,
        xanchor="left",
        y=1.15,
        yanchor="top",
        showactive=True,
        active=0,
        bgcolor=THEME["cream"],
        bordercolor=THEME["navy"],
        font=dict(size=11, color=THEME["navy"]),
        buttons=buttons,
        pad=dict(r=5, t=0),
    )


def _btn(label: str, visible: list[bool]) -> dict[str, Any]:
    return dict(label=label, method="update", args=[{"visible": visible}])


def _cycle(palette: list[str], n: int) -> list[str]:
    return [palette[i % len(palette)] for i in range(n)]


def _coerce_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes(include=["object"]).columns:
        sample = df[col].dropna().head(5)
        if sample.empty:
            continue
        try:
            parsed = pd.to_datetime(sample, format="mixed")
            if parsed.notna().all():
                df[col] = pd.to_datetime(df[col], format="mixed")
        except (ValueError, TypeError):
            continue
    return df
