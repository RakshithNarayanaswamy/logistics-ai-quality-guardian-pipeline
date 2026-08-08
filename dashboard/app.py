"""Streamlit dashboard for the logistics pipeline.

Reads Snowflake directly (marts + the persisted AI reports), so it always shows
the latest completed batch — no DAG task needs to "refresh" it.

    streamlit run dashboard/app.py
"""
import os
import sys

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.metrics import compute_metrics  # noqa: E402
from pipeline.reports import (  # noqa: E402
    METRIC_REPORT,
    QUALITY_REPORT,
    get_latest_reports,
    get_report_history,
)

_BAR_COLOR = "#4c78a8"  # single hue — these bars encode magnitude, not identity

st.set_page_config(page_title="Logistics Pipeline", page_icon="📦", layout="wide")


@st.cache_data(ttl=300)
def load_metrics():
    return compute_metrics()


@st.cache_data(ttl=300)
def load_reports():
    return get_latest_reports()


@st.cache_data(ttl=300)
def load_history(report_type, limit=10):
    return get_report_history(report_type, limit)


st.title("📦 Logistics Pipeline")
st.caption("Batch metrics and AI-generated commentary from the latest run")

if st.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()

try:
    metrics = load_metrics()
    reports = load_reports()
except Exception as exc:  # noqa: BLE001 - surface any connection/query issue in the UI
    st.error(f"Could not load data from Snowflake: {exc}")
    st.stop()

# --- Headline numbers: single values, so tiles rather than charts -------------
col1, col2, col3, col4 = st.columns(4)
col1.metric("OTIF", f"{metrics['otif_pct']}%")
col2.metric("Avg lead time", f"{metrics['avg_lead_time_days']} days")
col3.metric("Delay rate", f"{metrics['delay_rate_pct']}%")
col4.metric("Stockout rate", f"{metrics['stockout_rate_pct']}%")

st.divider()

# --- AI commentary -----------------------------------------------------------
left, right = st.columns(2)

with left:
    st.subheader("Pipeline health")
    quality = reports.get(QUALITY_REPORT)
    if quality:
        st.info(quality["content"])
        st.caption(f"Generated {quality['run_ts']:%Y-%m-%d %H:%M} UTC")
    else:
        st.caption("No quality report yet — run the `quality_guardian` task.")

with right:
    st.subheader("Metric commentary")
    narrative = reports.get(METRIC_REPORT)
    if narrative:
        st.info(narrative["content"])
        st.caption(f"Generated {narrative['run_ts']:%Y-%m-%d %H:%M} UTC")
    else:
        st.caption("No metric report yet — run the `metric_assistant` task.")

st.divider()

# --- Carrier performance: several measures per entity, so a table ------------
st.subheader("Carrier performance")
carriers = pd.DataFrame(metrics["carrier_performance"])
delays = pd.DataFrame(metrics["delay_rate_by_carrier"])
if not carriers.empty:
    carriers = carriers.merge(delays, on="carrier_name", how="left")
    carriers.columns = ["Carrier", "OTIF %", "Avg cost", "Delay rate %"]
    st.dataframe(carriers, hide_index=True, width="stretch")

st.divider()

# --- Stage timings and forecast: magnitude comparisons, so single-hue bars ----
left, right = st.columns(2)

with left:
    st.subheader("Avg hours per stage")
    transitions = pd.DataFrame(metrics["status_transition_avg_hours"])
    if not transitions.empty:
        st.bar_chart(
            transitions.set_index("transition")["avg_hours"],
            color=_BAR_COLOR,
            horizontal=True,
            height=320,
        )
    else:
        st.caption("No status-transition data available.")

with right:
    st.subheader("7-day demand forecast — top 10 parts")
    forecast = pd.DataFrame(metrics["demand_forecast"])
    if not forecast.empty:
        top = forecast.nlargest(10, "next_7_day_total")
        st.bar_chart(
            top.set_index("part_name")["next_7_day_total"],
            color=_BAR_COLOR,
            horizontal=True,
            height=320,
        )
    else:
        st.caption("No forecast available — run the `train_demand_forecast` task.")

# --- History -----------------------------------------------------------------
with st.expander("Previous health reports"):
    for entry in load_history(QUALITY_REPORT, 10):
        st.markdown(f"**{entry['run_ts']:%Y-%m-%d %H:%M} UTC**")
        st.write(entry["content"])
        st.divider()
