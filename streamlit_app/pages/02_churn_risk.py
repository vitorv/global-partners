"""
Churn Risk Indicators Dashboard
PDF Question: Which metrics—such as days since last order, average order interval,
and spend trends—correlate with a higher churn risk?
Focus: Highlight threshold-based alerts (e.g., customers at risk) to prompt re-engagement actions.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from data_loader import load_customer_daily

st.title("Churn Risk Indicators Dashboard")
st.caption("**Question:** Which metrics — days since last order, average order "
           "interval, and spend trends — correlate with a higher churn risk?")

# ── Load Data ────────────────────────────────────────────────────────────────
df_snap = load_customer_daily()
latest_date = df_snap["date_key"].max()
latest = df_snap[df_snap["date_key"] == latest_date].copy()

CHURN_COLORS = {
    "Active": "#22c55e",
    "At Risk": "#f59e0b",
    "Churned": "#ef4444"
}

st.markdown(f"**Snapshot Date:** {latest_date.date()}")

# ── KPI Cards ────────────────────────────────────────────────────────────────
total = len(latest)
active = (latest["churn_risk_flag"] == "Active").sum()
at_risk = (latest["churn_risk_flag"] == "At Risk").sum()
churned = (latest["churn_risk_flag"] == "Churned").sum()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Customers", f"{total:,}")
col2.metric("Active (≤30d)", f"{active:,}", delta=f"{active/total*100:.1f}%")
col3.metric("At Risk (31–90d)", f"{at_risk:,}", delta=f"{at_risk/total*100:.1f}%", delta_color="inverse")
col4.metric("Churned (>90d)", f"{churned:,}", delta=f"{churned/total*100:.1f}%", delta_color="inverse")

st.markdown("---")

# ── Row 1: Donut + Days Since Last Order Histogram ───────────────────────────
col_a, col_b = st.columns([1, 2])

with col_a:
    st.subheader("Current Status Breakdown")
    status_counts = latest["churn_risk_flag"].value_counts().reset_index()
    fig_donut = px.pie(
        status_counts, values="count", names="churn_risk_flag", hole=0.5,
        color="churn_risk_flag", color_discrete_map=CHURN_COLORS,
        template="plotly_dark"
    )
    fig_donut.update_layout(height=400)
    st.plotly_chart(fig_donut, use_container_width=True)

with col_b:
    st.subheader("Days Since Last Order Distribution")
    fig_hist = px.histogram(
        latest, x="days_since_last_order", color="churn_risk_flag",
        nbins=80, color_discrete_map=CHURN_COLORS, template="plotly_dark",
        barmode="overlay", opacity=0.7
    )
    fig_hist.add_vline(x=30, line_dash="dash", line_color="white",
                       annotation_text="At Risk (30d)", annotation_position="top left")
    fig_hist.add_vline(x=90, line_dash="dash", line_color="white",
                       annotation_text="Churned (90d)", annotation_position="top left")
    fig_hist.update_layout(height=400)
    st.plotly_chart(fig_hist, use_container_width=True)

st.markdown("---")

# ── Row 2: Correlation Charts ────────────────────────────────────────────────
col_c, col_d = st.columns(2)

with col_c:
    st.subheader("Avg Order Gap by Churn Status")
    st.caption("Do customers with wider order gaps churn more?")
    gap_data = latest.groupby("churn_risk_flag")["avg_order_gap_days"].mean().reset_index()
    fig_gap = px.bar(
        gap_data, x="churn_risk_flag", y="avg_order_gap_days",
        color="churn_risk_flag", color_discrete_map=CHURN_COLORS,
        template="plotly_dark",
        category_orders={"churn_risk_flag": ["Active", "At Risk", "Churned"]}
    )
    fig_gap.update_layout(height=350, showlegend=False)
    st.plotly_chart(fig_gap, use_container_width=True)

with col_d:
    st.subheader("Avg Lifetime Value by Churn Status")
    st.caption("Are higher-value customers less likely to churn?")
    ltv_data = latest.groupby("churn_risk_flag")["cumulative_ltv"].mean().reset_index()
    fig_ltv = px.bar(
        ltv_data, x="churn_risk_flag", y="cumulative_ltv",
        color="churn_risk_flag", color_discrete_map=CHURN_COLORS,
        template="plotly_dark",
        category_orders={"churn_risk_flag": ["Active", "At Risk", "Churned"]}
    )
    fig_ltv.update_layout(height=350, showlegend=False)
    st.plotly_chart(fig_ltv, use_container_width=True)

st.markdown("---")

# ── Row 3: At-Risk Alert Table ────────────────────────────────────────────────
st.subheader("At-Risk Customer Alert Table")
st.caption("Customers flagged as 'At Risk' — actionable list for re-engagement campaigns.")

at_risk_df = latest[latest["churn_risk_flag"] == "At Risk"].copy()
at_risk_df = at_risk_df.nlargest(50, "cumulative_ltv")
at_risk_display = at_risk_df[[
    "user_id", "days_since_last_order", "cumulative_order_count",
    "cumulative_ltv", "avg_order_gap_days", "is_loyalty", "clv_segment"
]].rename(columns={
    "user_id": "Customer ID",
    "days_since_last_order": "Days Inactive",
    "cumulative_order_count": "Lifetime Orders",
    "cumulative_ltv": "Lifetime Value ($)",
    "avg_order_gap_days": "Avg Order Gap (days)",
    "is_loyalty": "Loyalty Member",
    "clv_segment": "CLV Segment"
})
st.dataframe(at_risk_display, use_container_width=True, hide_index=True)
