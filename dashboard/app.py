"""
dashboard/app.py
────────────────────────────────────────────────────────────────────────────
US Energy Generation Dashboard
Powered by: EIA API → Airflow ELT Pipeline → PostgreSQL → Streamlit
────────────────────────────────────────────────────────────────────────────
Run: streamlit run dashboard/app.py
"""

import os
import sys
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils.db_helpers import query_to_dataframe, get_row_count

# ── Page config ────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="US Energy Generation Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Color palette (consistent across charts) ──────────────────────────────────

FUEL_COLORS = {
    "Solar":       "#F5A623",
    "Wind":        "#4A90D9",
    "Natural Gas": "#9B9B9B",
    "Coal":        "#4A4A4A",
    "Nuclear":     "#7ED321",
    "Hydropower":  "#50C8E8",
}

# ── Data loading ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_generation_data() -> pd.DataFrame:
    df = query_to_dataframe("""
        SELECT month, fuel_type, fuel_label, generation_mwh
        FROM electricity_generation
        ORDER BY month ASC
    """)
    df["month"] = pd.to_datetime(df["month"])
    df["year"]  = df["month"].dt.year
    return df

@st.cache_data(ttl=3600)
def load_pipeline_runs() -> pd.DataFrame:
    return query_to_dataframe("""
        SELECT run_id, rows_fetched, rows_loaded, status, started_at, finished_at
        FROM pipeline_runs
        ORDER BY started_at DESC
        LIMIT 10
    """)

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/4/40/EIA_logo.png", width=80)
    st.title("⚡ Energy Dashboard")
    st.caption("Data: U.S. Energy Information Administration")
    st.divider()

    st.subheader("Filters")

    df_raw = load_generation_data()
    all_fuels  = sorted(df_raw["fuel_label"].dropna().unique())
    all_years  = sorted(df_raw["year"].unique())

    selected_fuels = st.multiselect(
        "Energy Sources",
        options=all_fuels,
        default=["Solar", "Wind", "Natural Gas", "Coal", "Nuclear"],
    )

    year_range = st.slider(
        "Year Range",
        min_value=int(all_years[0]),
        max_value=int(all_years[-1]),
        value=(int(all_years[0]), int(all_years[-1])),
    )

    st.divider()
    st.subheader("Pipeline Health")
    total_rows = get_row_count()
    st.metric("Total rows in DB", f"{total_rows:,}")

# ── Filter data ────────────────────────────────────────────────────────────────

df = df_raw[
    (df_raw["fuel_label"].isin(selected_fuels)) &
    (df_raw["year"] >= year_range[0]) &
    (df_raw["year"] <= year_range[1])
].copy()

# ── Header ─────────────────────────────────────────────────────────────────────

st.title("⚡ US Electricity Generation by Source")
st.caption(
    f"Data: U.S. Energy Information Administration (EIA) Open Data · "
    f"Pipeline: Apache Airflow · "
    f"Showing {year_range[0]}–{year_range[1]}"
)
st.divider()

# ── KPI row ────────────────────────────────────────────────────────────────────

col1, col2, col3, col4 = st.columns(4)

latest_month = df["month"].max()
latest_data  = df[df["month"] == latest_month]
latest_total = latest_data["generation_mwh"].sum()

renewable_mwh = latest_data[
    latest_data["fuel_type"].isin(["SUN", "WND", "HYC"])
]["generation_mwh"].sum()

renewable_pct = (renewable_mwh / latest_total * 100) if latest_total > 0 else 0

solar_row  = df[df["fuel_type"] == "SUN"].groupby("year")["generation_mwh"].sum()
solar_yoy  = ((solar_row.iloc[-1] - solar_row.iloc[-2]) / solar_row.iloc[-2] * 100) if len(solar_row) >= 2 else 0

with col1:
    st.metric("Latest Month", latest_month.strftime("%b %Y"))
with col2:
    st.metric("Total Generation", f"{latest_total / 1e6:.2f} TWh")
with col3:
    st.metric("Renewable Share", f"{renewable_pct:.1f}%")
with col4:
    st.metric("Solar YoY Growth", f"{solar_yoy:+.1f}%")

st.divider()

# ── Chart 1: Generation over time ──────────────────────────────────────────────

st.subheader("Monthly Generation Over Time")

fig1 = px.area(
    df,
    x="month",
    y="generation_mwh",
    color="fuel_label",
    color_discrete_map=FUEL_COLORS,
    labels={
        "generation_mwh": "Generation (MWh)",
        "month":          "Month",
        "fuel_label":     "Source",
    },
    title="",
)
fig1.update_layout(
    legend_title_text="Energy Source",
    hovermode="x unified",
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
)
st.plotly_chart(fig1, use_container_width=True)

# ── Charts 2 & 3: Pie + Bar side by side ──────────────────────────────────────

col_left, col_right = st.columns(2)

with col_left:
    st.subheader(f"Generation Mix — {latest_month.strftime('%b %Y')}")
    fig2 = px.pie(
        latest_data,
        values="generation_mwh",
        names="fuel_label",
        color="fuel_label",
        color_discrete_map=FUEL_COLORS,
        hole=0.45,
    )
    fig2.update_traces(textposition="inside", textinfo="percent+label")
    fig2.update_layout(showlegend=False)
    st.plotly_chart(fig2, use_container_width=True)

with col_right:
    st.subheader("Annual Generation by Source")
    yearly = (
        df.groupby(["year", "fuel_label"])["generation_mwh"]
        .sum()
        .reset_index()
    )
    fig3 = px.bar(
        yearly,
        x="year",
        y="generation_mwh",
        color="fuel_label",
        color_discrete_map=FUEL_COLORS,
        barmode="group",
        labels={
            "generation_mwh": "Generation (MWh)",
            "year":           "Year",
            "fuel_label":     "Source",
        },
    )
    fig3.update_layout(
        legend_title_text="Source",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig3, use_container_width=True)

# ── Chart 4: Renewable share trend ────────────────────────────────────────────

st.subheader("Renewable Energy Share Over Time (%)")

monthly_total = df.groupby("month")["generation_mwh"].sum().reset_index(name="total")
monthly_ren   = (
    df[df["fuel_type"].isin(["SUN", "WND", "HYC"])]
    .groupby("month")["generation_mwh"].sum()
    .reset_index(name="renewable")
)
share_df = monthly_total.merge(monthly_ren, on="month", how="left").fillna(0)
share_df["renewable_pct"] = share_df["renewable"] / share_df["total"] * 100

fig4 = px.line(
    share_df,
    x="month",
    y="renewable_pct",
    labels={"renewable_pct": "Renewable Share (%)", "month": "Month"},
    color_discrete_sequence=["#7ED321"],
)
fig4.add_hline(y=share_df["renewable_pct"].mean(), line_dash="dash",
               annotation_text="Average", line_color="gray")
fig4.update_layout(plot_bgcolor="rgba(0,0,0,0)")
st.plotly_chart(fig4, use_container_width=True)

# ── Pipeline run log ───────────────────────────────────────────────────────────

with st.expander("🔧 Pipeline Run History"):
    try:
        runs_df = load_pipeline_runs()
        st.dataframe(runs_df, use_container_width=True)
    except Exception:
        st.info("No pipeline runs logged yet. Trigger the Airflow DAG to populate this.")

# ── Footer ─────────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    "Built with Apache Airflow · PostgreSQL · Streamlit · Plotly  |  "
    "Data: EIA Open Data API  |  "
    "Source: github.com/YOUR_USERNAME/eia-airflow-pipeline"
)
