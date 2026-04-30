import streamlit as st
import pandas as pd
import plotly.express as px
from data_loader import load_customer_daily

# --- Load CSS ---
with open("streamlit_app/style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.title("⚠️ Churn Analysis")

df_snap = load_customer_daily()
latest_date = df_snap["date_key"].max()
latest_snap = df_snap[df_snap["date_key"] == latest_date]

st.markdown(f"**As of {latest_date.date()}**")

# Layout
col1, col2 = st.columns([1, 2])

color_map = {
    "Active": "#22c55e",
    "At Risk": "#f59e0b",
    "Churned": "#ef4444"
}

with col1:
    st.subheader("Current Status Breakdown")
    status_counts = latest_snap["churn_risk_flag"].value_counts().reset_index()
    fig_donut = px.pie(
        status_counts, 
        values="count", 
        names="churn_risk_flag", 
        hole=0.5,
        color="churn_risk_flag",
        color_discrete_map=color_map,
        template="plotly_dark"
    )
    st.plotly_chart(fig_donut, use_container_width=True)

with col2:
    st.subheader("Days Since Last Order Distribution")
    fig_hist = px.histogram(
        latest_snap, 
        x="days_since_last_order", 
        color="churn_risk_flag",
        nbins=100,
        color_discrete_map=color_map,
        template="plotly_dark"
    )
    fig_hist.add_vline(x=30, line_dash="dash", line_color="white", annotation_text="At Risk (30d)")
    fig_hist.add_vline(x=90, line_dash="dash", line_color="white", annotation_text="Churned (90d)")
    st.plotly_chart(fig_hist, use_container_width=True)

st.markdown("---")
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Average Order Gap by CLV Segment")
    gap_data = latest_snap.groupby("clv_segment")["avg_order_gap_days"].mean().reset_index()
    fig_gap = px.bar(
        gap_data,
        x="clv_segment",
        y="avg_order_gap_days",
        color="clv_segment",
        template="plotly_dark",
        color_discrete_map={"High": "#22c55e", "Medium": "#8b5cf6", "Low": "#64748b"}
    )
    st.plotly_chart(fig_gap, use_container_width=True)

with col_b:
    st.subheader("Loyalty vs Non-Loyalty Churn")
    loyalty_counts = latest_snap.groupby(["is_loyalty", "churn_risk_flag"]).size().reset_index(name="count")
    loyalty_counts["is_loyalty"] = loyalty_counts["is_loyalty"].map({True: "Loyalty Member", False: "Non-Member"})
    
    fig_loyalty = px.bar(
        loyalty_counts,
        x="is_loyalty",
        y="count",
        color="churn_risk_flag",
        barmode="stack",
        template="plotly_dark",
        color_discrete_map=color_map
    )
    st.plotly_chart(fig_loyalty, use_container_width=True)
