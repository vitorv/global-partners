"""
Customer Segmentation Dashboard
PDF Question: What distinct customer segments emerge when grouping customers
by purchase behavior (total spend, frequency, recency) and loyalty status?
Focus: Visualize segmentation (e.g., via RFM scores) to enable targeted marketing.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from data_loader import load_customer_rfm, load_dim_customer

st.title("Customer Segmentation Dashboard")
st.caption("**Question:** What distinct customer segments emerge when grouping "
           "customers by purchase behavior (total spend, frequency, recency) "
           "and loyalty status?")

# ── Load Data ────────────────────────────────────────────────────────────────
df_rfm = load_customer_rfm()
df_cust = load_dim_customer()

# Merge loyalty status onto RFM
df = df_rfm.merge(df_cust[["user_id", "is_loyalty"]], on="user_id", how="left")
df["loyalty_status"] = df["is_loyalty"].map({True: "Loyalty Member", False: "Non-Member"})

SEGMENT_COLORS = {
    "VIP": "#22c55e",
    "Regular": "#8b5cf6",
    "New Customer": "#64748b",
    "Churn Risk": "#ef4444"
}

# ── KPI Cards ────────────────────────────────────────────────────────────────
total_customers = len(df)
vip_pct = (df["rfm_segment"] == "VIP").sum() / total_customers * 100
loyalty_pct = (df["is_loyalty"] == True).sum() / total_customers * 100
churn_risk_pct = (df["rfm_segment"] == "Churn Risk").sum() / total_customers * 100

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Customers", f"{total_customers:,}")
col2.metric("VIP Customers", f"{vip_pct:.1f}%")
col3.metric("Loyalty Members", f"{loyalty_pct:.1f}%")
col4.metric("Churn Risk", f"{churn_risk_pct:.1f}%")

st.markdown("---")

# ── Row 1: RFM Scatter + Segment Donut ───────────────────────────────────────
col_a, col_b = st.columns([3, 2])

with col_a:
    st.subheader("RFM Scatter Plot (Frequency vs Monetary)")
    plot_df = df.sample(n=min(2000, len(df)), random_state=42).copy()
    plot_df["bubble_size"] = 1 / (plot_df["recency_days"] + 1)

    fig_scatter = px.scatter(
        plot_df,
        x="frequency",
        y="monetary",
        color="rfm_segment",
        size="bubble_size",
        hover_data=["user_id", "recency_days", "loyalty_status"],
        color_discrete_map=SEGMENT_COLORS,
        template="plotly_dark",
        title="Sample of 2,000 Customers (bubble size = recency)",
        labels={
            "frequency": "Frequency (Total Lifetime Orders)",
            "monetary": "Monetary (Total Lifetime Spend $)"
        }
    )
    fig_scatter.update_layout(height=450)
    st.plotly_chart(fig_scatter, use_container_width=True)

with col_b:
    st.subheader("Segment Distribution")
    counts = df["rfm_segment"].value_counts().reset_index()
    fig_donut = px.pie(
        counts, values="count", names="rfm_segment", hole=0.5,
        color="rfm_segment", color_discrete_map=SEGMENT_COLORS,
        template="plotly_dark"
    )
    fig_donut.update_layout(height=450)
    st.plotly_chart(fig_donut, use_container_width=True)

st.markdown("---")

# ── Row 2: Segment × Loyalty Stacked Bar ─────────────────────────────────────
st.subheader("Segments by Loyalty Status")
st.caption("How do loyalty members distribute across the RFM segments compared to non-members?")

cross = df.groupby(["rfm_segment", "loyalty_status"]).size().reset_index(name="count")
fig_stacked = px.bar(
    cross,
    x="rfm_segment",
    y="count",
    color="loyalty_status",
    barmode="group",
    template="plotly_dark",
    color_discrete_map={"Loyalty Member": "#22c55e", "Non-Member": "#64748b"},
    category_orders={"rfm_segment": ["VIP", "Regular", "New Customer", "Churn Risk"]}
)
fig_stacked.update_layout(height=400)
st.plotly_chart(fig_stacked, use_container_width=True)

st.markdown("---")

# ── Row 3: Segment Average Scores Table ───────────────────────────────────────
st.subheader("Segment Average Scores")
avg_scores = df.groupby("rfm_segment").agg(
    Customers=("user_id", "count"),
    Avg_Recency_Days=("recency_days", "mean"),
    Avg_Frequency=("frequency", "mean"),
    Avg_Monetary=("monetary", "mean"),
    Loyalty_Rate=("is_loyalty", "mean"),
).round(2).reset_index()
avg_scores["Loyalty_Rate"] = (avg_scores["Loyalty_Rate"] * 100).round(1).astype(str) + "%"
avg_scores = avg_scores.rename(columns={"rfm_segment": "Segment"})
st.dataframe(avg_scores, use_container_width=True, hide_index=True)
