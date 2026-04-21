"""
SimpleClosure take-home — flight delays dashboard.

- Loads the pre-aggregated parquet from S3 on startup
- Serves 4 charts + a KPI row
- Controls: date range, aggregation (w/m/q/y), carriers, delay basis
  (Arrival / Departure / Either), and an Airport ↔ City grain toggle
"""

import ctypes
import gc
import os

import pandas as pd
import pyarrow.parquet as pq
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash_bootstrap_components as dbc
from dash import Dash, dcc, html, Input, Output, State


def _trim_malloc():
    # Force the Linux allocator to return freed memory to the OS.
    # No-ops on macOS / Windows.
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except (OSError, AttributeError):
        pass

# ── Data ──

AGG_URL = os.environ.get(
    "AGG_URL",
    "https://s3-flights-harman-simpleclosure.s3.amazonaws.com/flights_agg.parquet",
)

# Read via pyarrow and dict-encode strings BEFORE converting to pandas.
# This avoids pandas briefly materializing string columns as object dtype
# (which balloons RAM ~5×, enough to OOM a 512 MB container).
print(f"Loading agg parquet from {AGG_URL} ...")
table = pq.read_table(AGG_URL)
for col in ["carrier", "Origin", "Dest", "OriginCityName",
            "OriginState", "DestCityName", "DestState"]:
    idx = table.schema.get_field_index(col)
    table = table.set_column(idx, col, table.column(col).dictionary_encode())
df = table.to_pandas()
del table
gc.collect()
_trim_malloc()

df["week_start"] = pd.to_datetime(df["week_start"])
df["n_flights"] = df["n_flights"].astype("int32")
for col in ["n_delayed_arr", "n_delayed_dep", "n_delayed_either",
            "n_cancelled", "n_diverted", "sum_arr_delay_min",
            "sum_carrier_delay", "sum_weather_delay", "sum_nas_delay",
            "sum_security_delay", "sum_late_aircraft_delay"]:
    df[col] = df[col].astype("float32")

gc.collect()
_trim_malloc()

mem_mb = df.memory_usage(deep=True).sum() / 1e6
print(f"Loaded {len(df):,} rows, "
      f"{df['week_start'].min().date()} → {df['week_start'].max().date()}, "
      f"{mem_mb:.0f} MB in memory")

CAUSE_COLS = {
    "Carrier":        "sum_carrier_delay",
    "Late Aircraft":  "sum_late_aircraft_delay",
    "NAS":            "sum_nas_delay",
    "Weather":        "sum_weather_delay",
    "Security":       "sum_security_delay",
}
CAUSE_COLORS = {
    "Carrier":       "#F97066",
    "Late Aircraft": "#F79009",
    "NAS":           "#36BFFA",
    "Weather":       "#2E90FA",
    "Security":      "#7A5AF8",
}

# Delay-basis toggle → which pre-computed "delayed" column to sum
DELAY_BASIS_COL = {
    "either":    "n_delayed_either",
    "arrival":   "n_delayed_arr",
    "departure": "n_delayed_dep",
}

# Build slider index ↔ week_start lookup
ALL_WEEKS = sorted(df["week_start"].unique())
WEEK_IDX = {w: i for i, w in enumerate(ALL_WEEKS)}
MIN_IDX, MAX_IDX = 0, len(ALL_WEEKS) - 1

# Slider tick marks — Jan + Jul of each year
slider_marks = {}
for w in ALL_WEEKS:
    ts = pd.Timestamp(w)
    if ts.day <= 7 and ts.month in (1, 7):
        slider_marks[WEEK_IDX[w]] = {"label": f"{ts.strftime('%b')} '{str(ts.year)[-2:]}"}

CARRIERS = sorted(df["carrier"].unique().tolist())

# ── Helpers ──

FREQ_MAP = {"week": "W-SUN", "month": "M", "quarter": "Q", "year": "Y"}


def resample_time(frame: pd.DataFrame, freq: str) -> pd.DataFrame:
    # Bucket week_start up to month/quarter/year. Week passes through.
    if freq == "week":
        return frame.assign(period=frame["week_start"])
    period = frame["week_start"].dt.to_period(FREQ_MAP[freq]).dt.start_time
    return frame.assign(period=period)


def filter_df(idx_range, carriers):
    start = ALL_WEEKS[idx_range[0]]
    end   = ALL_WEEKS[idx_range[1]]
    sub = df[(df["week_start"] >= start) & (df["week_start"] <= end)]
    if carriers:
        sub = sub[sub["carrier"].isin(carriers)]
    return sub


def style_fig(fig, title=None):
    # Single place to apply the dark theme + hover styling to every chart
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin={"t": 50 if title else 20, "l": 40, "r": 20, "b": 40},
        legend={"orientation": "h", "y": -0.18, "x": 0, "font": {"size": 11}},
        font={"color": "#E5E7EB", "family": "-apple-system, BlinkMacSystemFont, system-ui, sans-serif"},
        hoverlabel={
            "bgcolor": "#111827",
            "bordercolor": "rgba(54, 191, 250, 0.4)",
            "font": {"color": "#F9FAFB", "family": "system-ui", "size": 12},
            "align": "left",
        },
    )
    if title:
        fig.update_layout(title={"text": title, "x": 0.0, "xanchor": "left", "y": 0.97,
                                 "font": {"size": 14, "color": "#F9FAFB"}})
    fig.update_xaxes(gridcolor="rgba(255,255,255,0.05)", zerolinecolor="rgba(255,255,255,0.05)")
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.05)", zerolinecolor="rgba(255,255,255,0.05)")
    return fig


# ── App ──

app = Dash(
    __name__,
    title="Flight Delays — SimpleClosure",
    external_stylesheets=[dbc.themes.DARKLY],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
server = app.server


def kpi_card(label, value, accent="#36BFFA"):
    return dbc.Col(
        dbc.Card(
            dbc.CardBody([
                html.Div(label, className="kpi-label"),
                html.Div(value, className="kpi-value", style={"color": accent}),
            ]),
            className="kpi-card h-100",
            style={"--accent": accent},
        ),
        md=3, sm=6, xs=12,
    )


controls = dbc.Card(
    dbc.CardBody([
        # Row 1: date range slider (full width)
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.Label("Date range", className="form-label d-inline-block me-3 mb-0"),
                    html.Span(id="date-pill-start", className="date-pill"),
                    html.Span("→", className="date-pill-arrow"),
                    html.Span(id="date-pill-end", className="date-pill"),
                ], className="d-flex align-items-center mb-2"),
                dcc.RangeSlider(
                    id="date-range",
                    min=MIN_IDX, max=MAX_IDX,
                    value=[MIN_IDX, MAX_IDX],
                    marks=slider_marks,
                    step=1,
                    allowCross=False,
                    tooltip={
                        "placement": "bottom",
                        "always_visible": False,
                        "template": "{value}",
                        "transform": "weekToDate",
                    },
                ),
            ]),
        ], className="mb-4"),
        # Row 2: aggregation + delay basis
        dbc.Row([
            dbc.Col([
                html.Label("Aggregation", className="form-label"),
                dbc.RadioItems(
                    id="freq",
                    options=[{"label": f.title(), "value": f} for f in FREQ_MAP],
                    value="month",
                    inline=True,
                    inputClassName="btn-check",
                    labelClassName="btn btn-outline-primary btn-sm",
                    labelCheckedClassName="active",
                    className="btn-group",
                ),
            ], md=5),
            dbc.Col([
                html.Label("Delay basis", className="form-label"),
                dbc.RadioItems(
                    id="delay-basis",
                    options=[{"label": "Either", "value": "either"},
                             {"label": "Arrival", "value": "arrival"},
                             {"label": "Departure", "value": "departure"}],
                    value="either",
                    inline=True,
                    inputClassName="btn-check",
                    labelClassName="btn btn-outline-primary btn-sm",
                    labelCheckedClassName="active",
                    className="btn-group",
                ),
            ], md=7),
        ], className="g-3 mb-3"),
        # Row 3: carriers + actions
        dbc.Row([
            dbc.Col([
                html.Label("Carriers", className="form-label"),
                dcc.Dropdown(
                    id="carriers",
                    options=[{"label": c, "value": c} for c in CARRIERS],
                    multi=True,
                    placeholder="All carriers",
                ),
            ], md=8),
            dbc.Col([
                html.Label("Actions", className="form-label"),
                html.Div([
                    dbc.Button("↺ Reset", id="reset-btn", color="secondary",
                               outline=True, size="sm", className="me-2"),
                    dbc.Button(
                        id="download-btn",
                        color="primary",
                        outline=True,
                        size="sm",
                        children=[
                            dcc.Loading(
                                html.Span("↓ CSV", id="download-btn-label"),
                                type="dot",
                                color="#36BFFA",
                                delay_show=50,
                            ),
                        ],
                    ),
                    dcc.Download(id="download-data"),
                ], className="d-flex"),
            ], md=4),
        ], className="g-3"),
    ]),
    className="mb-4 controls-card",
)


def graph_card(graph_id):
    return dbc.Card(
        dbc.CardBody(
            dcc.Loading(
                dcc.Graph(id=graph_id, config={"displayModeBar": False},
                          style={"height": "380px"}),
                type="default",
                color="#36BFFA",
            )
        ),
    )


app.layout = dbc.Container(
    fluid=True,
    style={"maxWidth": "1500px", "padding": "40px 32px"},
    children=[
        html.Div([
            html.Div([
                html.Span("▲ ", style={"color": "#36BFFA"}),
                html.Span("BTS Marketing Carrier On-Time Performance", className="text-muted small"),
            ], className="mb-2"),
            html.H1("US Flight Delays", className="fw-bold mb-1", style={"fontSize": "2.4rem"}),
            html.P(
                "Jan 2018 → Jan 2025 · A delay = arrival or departure more than 15 minutes late",
                className="text-muted mb-4",
            ),
        ]),
        controls,

        # Section header: aggregate metrics across the current selection
        html.Div("Aggregate metrics", className="text-muted small mb-2",
                 style={"textTransform": "uppercase", "letterSpacing": "0.08em"}),
        dcc.Loading(
            dbc.Row(id="kpi-row", className="mb-4 g-3"),
            type="default",
            color="#36BFFA",
        ),

        # Section header: minute-level breakdowns (cause + carrier)
        html.Div([
            html.Div("Minute-level breakdown",
                     style={"textTransform": "uppercase", "letterSpacing": "0.08em"}),
            html.Div("Arrival-only — BTS doesn't publish per-cause or per-minute "
                     "attribution for departure delays",
                     style={"textTransform": "none", "letterSpacing": "normal",
                            "fontSize": "0.75rem", "opacity": 0.7, "marginTop": "2px"}),
        ], className="text-muted small mt-4 mb-2"),
        dbc.Row([
            dbc.Col(graph_card("chart-cause"),   md=7),
            dbc.Col(graph_card("chart-carrier"), md=5),
        ], className="g-3 mb-3"),

        # Section header: geography (respects Grain toggle)
        html.Div([
            html.Div("Location breakdown", className="text-muted small",
                     style={"textTransform": "uppercase", "letterSpacing": "0.08em"}),
            dbc.RadioItems(
                id="grain",
                options=[{"label": "Airport", "value": "airport"},
                         {"label": "City", "value": "city"}],
                value="airport",
                inline=True,
                inputClassName="btn-check",
                labelClassName="btn btn-outline-primary btn-sm",
                labelCheckedClassName="active",
                className="btn-group",
            ),
        ], className="d-flex justify-content-between align-items-center mt-4 mb-2"),
        dbc.Row([
            dbc.Col(graph_card("chart-origin"), md=6),
            dbc.Col(graph_card("chart-dest"),   md=6),
        ], className="g-3"),
    ],
)

# ── Callbacks ──


@app.callback(
    Output("kpi-row",          "children"),
    Output("chart-cause",      "figure"),
    Output("chart-carrier",    "figure"),
    Output("chart-origin",     "figure"),
    Output("chart-dest",       "figure"),
    Output("date-pill-start",  "children"),
    Output("date-pill-end",    "children"),
    Input("date-range",  "value"),
    Input("freq",        "value"),
    Input("carriers",    "value"),
    Input("grain",       "value"),
    Input("delay-basis", "value"),
)
def update_charts(idx_range, freq, carriers, grain, delay_basis):
    delay_col = DELAY_BASIS_COL.get(delay_basis, "n_delayed_either")
    start_dt = pd.Timestamp(ALL_WEEKS[idx_range[0]])
    end_dt   = pd.Timestamp(ALL_WEEKS[idx_range[1]])
    start_label = start_dt.strftime("%b %d, %Y")
    end_label   = end_dt.strftime("%b %d, %Y")

    sub = filter_df(idx_range, carriers or [])
    if sub.empty:
        empty = style_fig(px.scatter(), "No data for selection")
        return [], empty, empty, empty, empty, start_label, end_label

    # KPIs
    # - Delay rate uses the chosen basis column
    # - Cancellation rate is basis-independent
    # - Avg delay minutes uses arrival magnitude (only minute-level we kept)
    total_flights  = int(sub["n_flights"].sum())
    total_delayed  = int(sub[delay_col].sum())
    total_cancel   = int(sub["n_cancelled"].sum())
    delay_rate     = total_delayed / total_flights if total_flights else 0
    cancel_rate    = total_cancel / total_flights if total_flights else 0
    avg_delay_min  = sub["sum_arr_delay_min"].sum() / max(int(sub["n_delayed_arr"].sum()), 1)

    kpis = [
        kpi_card("Flights",
                 f"{total_flights / 1e6:.1f}M" if total_flights >= 1e6 else f"{total_flights:,}",
                 accent="#36BFFA"),
        kpi_card("Delay rate",          f"{delay_rate:.1%}",       accent="#F97066"),
        kpi_card("Cancellation rate",   f"{cancel_rate:.2%}",      accent="#F79009"),
        kpi_card("Avg delay (when late)", f"{avg_delay_min:.0f} min", accent="#7A5AF8"),
    ]

    # Chart 1 — stacked area of cause minutes over time
    # Arrival-attributed only (BTS data limitation, noted in subtitle)
    time_df = resample_time(sub, freq)
    cause_df = (
        time_df.groupby("period")[list(CAUSE_COLS.values())].sum()
               .rename(columns={v: k for k, v in CAUSE_COLS.items()})
               .reset_index()
               .melt(id_vars="period", var_name="Cause", value_name="Minutes")
    )
    fig_cause = px.area(
        cause_df, x="period", y="Minutes", color="Cause",
        color_discrete_map=CAUSE_COLORS,
        category_orders={"Cause": list(CAUSE_COLS.keys())},
    )
    fig_cause.update_traces(line={"width": 0}, hovertemplate="%{fullData.name}: %{y:,.0f} min<extra></extra>")
    fig_cause = style_fig(fig_cause, "Delay minutes by cause")

    tickformat_by_freq = {"week": "%b %d, '%y", "month": "%b '%y", "quarter": "Q%q %Y", "year": "%Y"}
    fig_cause.update_layout(hovermode="x unified", yaxis_title=None, xaxis_title=None)
    fig_cause.update_xaxes(
        tickformat=tickformat_by_freq.get(freq, "%b '%y"),
        nticks=10, ticklabelmode="period",
        showline=True, linecolor="rgba(255,255,255,0.1)",
    )
    fig_cause.update_yaxes(
        tickformat=",.0s",
        showline=True, linecolor="rgba(255,255,255,0.1)",
    )

    # Chart 2 — carrier bubble
    # - X: delay rate (respects basis toggle)
    # - Y: avg arrival delay minutes (arrival-only, BTS limitation)
    # - Size: flight volume
    carrier_df = (
        sub.groupby("carrier", as_index=False, observed=True)
           .agg(n_flights=("n_flights", "sum"),
                n_delayed=(delay_col, "sum"),
                n_delayed_arr=("n_delayed_arr", "sum"),
                sum_delay=("sum_arr_delay_min", "sum"))
    )
    carrier_df["delay_rate"] = carrier_df["n_delayed"] / carrier_df["n_flights"]
    carrier_df["avg_delay_min"] = carrier_df["sum_delay"] / carrier_df["n_delayed_arr"].replace(0, 1)

    fig_carrier = go.Figure()
    fig_carrier.add_trace(go.Scatter(
        x=carrier_df["delay_rate"],
        y=carrier_df["avg_delay_min"],
        mode="markers+text",
        text=carrier_df["carrier"],
        textposition="middle center",
        textfont={"color": "white", "size": 10, "family": "system-ui"},
        marker={
            "size": carrier_df["n_flights"],
            "sizemode": "area",
            "sizeref": 2.0 * carrier_df["n_flights"].max() / (55.0 ** 2),
            "sizemin": 14,
            "color": carrier_df["delay_rate"],
            "colorscale": [[0, "#12B76A"], [0.5, "#F79009"], [1, "#F97066"]],
            "line": {"width": 0},
            "opacity": 0.85,
        },
        customdata=carrier_df[["n_flights", "delay_rate", "avg_delay_min"]].values,
        hovertemplate=(
            "<b>%{text}</b><br>"
            "Flights: %{customdata[0]:,}<br>"
            "Delay rate: %{customdata[1]:.1%}<br>"
            "Avg delay: %{customdata[2]:.0f} min<extra></extra>"
        ),
    ))
    fig_carrier = style_fig(fig_carrier, "Carriers — frequency × severity of delays")
    fig_carrier.update_xaxes(title="Delay rate", tickformat=".0%", nticks=6,
                             showline=True, linecolor="rgba(255,255,255,0.1)")
    fig_carrier.update_yaxes(title="Avg minutes late", ticksuffix=" min", nticks=6,
                             showline=True, linecolor="rgba(255,255,255,0.1)")

    # Charts 3 & 4 — paired best/worst bars for origin + destination
    if grain == "city":
        fig_origin = paired_airport_chart(sub, "OriginCityName", None,
                                          "Origin cities · best vs worst", delay_col)
        fig_dest   = paired_airport_chart(sub, "DestCityName",   None,
                                          "Destination cities · best vs worst", delay_col)
    else:
        fig_origin = paired_airport_chart(sub, "Origin", "OriginCityName",
                                          "Origin airports · best vs worst", delay_col)
        fig_dest   = paired_airport_chart(sub, "Dest",   "DestCityName",
                                          "Destination airports · best vs worst", delay_col)

    return kpis, fig_cause, fig_carrier, fig_origin, fig_dest, start_label, end_label


@app.callback(
    Output("date-range",  "value"),
    Output("freq",        "value"),
    Output("carriers",    "value"),
    Output("grain",       "value"),
    Output("delay-basis", "value"),
    Input("reset-btn",    "n_clicks"),
    prevent_initial_call=True,
)
def reset_filters(_):
    return [MIN_IDX, MAX_IDX], "month", [], "airport", "either"


@app.callback(
    Output("download-data",      "data"),
    Output("download-btn-label", "children"),
    Input("download-btn", "n_clicks"),
    State("date-range", "value"),
    State("carriers",   "value"),
    prevent_initial_call=True,
)
def download_csv(n_clicks, idx_range, carriers):
    sub = filter_df(idx_range, carriers or [])
    start = pd.Timestamp(ALL_WEEKS[idx_range[0]]).date()
    end   = pd.Timestamp(ALL_WEEKS[idx_range[1]]).date()
    fname = f"flights_agg_{start}_to_{end}.csv"
    print(f"[download] generating {fname} ({len(sub):,} rows)...")
    return dcc.send_data_frame(sub.to_csv, fname, index=False), "↓ CSV"


def paired_airport_chart(sub, code_col, city_col, title, delay_col):
    # - Group by airport code (or city), keep only buckets with ≥500 flights in window
    # - Top 10 lowest + top 10 highest delay rate, rendered as side-by-side subplots
    group_cols = [code_col] if city_col is None else [code_col, city_col]
    agg = (
        sub.groupby(group_cols, as_index=False, observed=True)
           .agg(n_flights=("n_flights", "sum"), n_delayed=(delay_col, "sum"))
    )
    agg = agg[agg["n_flights"] >= 500]
    agg["delay_rate"] = agg["n_delayed"] / agg["n_flights"]

    best  = agg.nsmallest(10, "delay_rate").sort_values("delay_rate", ascending=False)
    worst = agg.nlargest(10, "delay_rate").sort_values("delay_rate", ascending=True)

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("<span style='color:#12B76A'>Best 10</span>",
                        "<span style='color:#F97066'>Worst 10</span>"),
        horizontal_spacing=0.18,
    )

    for col_idx, (data, color) in enumerate([(best, "#12B76A"), (worst, "#F97066")], start=1):
        if city_col:
            customdata = data[[city_col, "n_flights"]].values
            hovertemplate = "<b>%{y}</b> — %{customdata[0]}<br>Flights: %{customdata[1]:,}<br>Delay rate: %{x:.1%}<extra></extra>"
        else:
            customdata = data[["n_flights"]].values
            hovertemplate = "<b>%{y}</b><br>Flights: %{customdata[0]:,}<br>Delay rate: %{x:.1%}<extra></extra>"
        fig.add_trace(
            go.Bar(
                x=data["delay_rate"], y=data[code_col], orientation="h",
                marker={"color": color, "line": {"width": 0}},
                customdata=customdata,
                hovertemplate=hovertemplate,
                text=[f"{v:.1%}" for v in data["delay_rate"]],
                textposition="outside",
                textfont={"color": "#E5E7EB", "size": 11},
                cliponaxis=False,
                showlegend=False,
            ),
            row=1, col=col_idx,
        )

    fig.update_xaxes(
        tickformat=".0%", nticks=5,
        showline=True, linecolor="rgba(255,255,255,0.1)",
        range=[0, max(agg["delay_rate"].max() * 1.15, 0.05)],  # headroom for outside labels
        fixedrange=True,
    )
    fig.update_yaxes(
        showline=True, linecolor="rgba(255,255,255,0.1)",
        fixedrange=True,
    )
    for annot in fig.layout.annotations:
        annot.font.size = 12
    fig = style_fig(fig, title)
    fig.update_layout(margin={"t": 60, "l": 40, "r": 40, "b": 40})
    return fig


if __name__ == "__main__":
    app.run(
        debug=True,
        dev_tools_ui=False,
        dev_tools_props_check=False,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8050)),
    )
