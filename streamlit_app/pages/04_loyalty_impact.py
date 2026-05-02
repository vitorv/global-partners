"""
Loyalty Program Impact Dashboard
PDF Question: How does loyalty membership affect customer spending and repeat order rates?
Focus: Compare key metrics (CLV, average order value, repeat purchase rate) between
loyalty and non-loyalty customers to assess program effectiveness and inform
potential adjustments.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from data_loader import load_order_summary, load_customer_daily, load_dim_customer

st.title("Loyalty Program Impact Dashboard")
st.caption("**Question:** How does loyalty membership affect customer spending "
           "and repeat order rates?")

# ── Load Data ────────────────────────────────────────────────────────────────
df_orders = load_order_summary()
df_snap = load_customer_daily()
df_cust = load_dim_customer()

latest_date = df_snap["date_key"].max()
latest = df_snap[df_snap["date_key"] == latest_date].copy()

LOYALTY_COLORS = {"Loyalty Member": "#22c55e", "Non-Member": "#64748b"}

# ── Derive per-customer metrics from order summary ───────────────────────────
user_orders = df_orders.groupby("user_id").agg(
    total_orders=("order_id", "count"),
    total_revenue=("net_revenue", "sum"),
    avg_order_value=("net_revenue", "mean"),
).reset_index()
user_orders = user_orders.merge(df_cust[["user_id", "is_loyalty"]], on="user_id", how="left")
user_orders["loyalty_status"] = user_orders["is_loyalty"].map({True: "Loyalty Member", False: "Non-Member"})
user_orders["is_repeat"] = user_orders["total_orders"] > 1

# ── KPI Cards: Loyalty vs Non-Loyalty ─────────────────────────────────────────
loyalty = user_orders[user_orders["is_loyalty"] == True]
non_loyalty = user_orders[user_orders["is_loyalty"] == False]

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 💚 Loyalty Members")
    c1, c2, c3 = st.columns(3)
    c1.metric("Avg CLV", f"${loyalty['total_revenue'].mean():,.2f}")
    c2.metric("Avg Order Value", f"${loyalty['avg_order_value'].mean():,.2f}")
    c3.metric("Repeat Purchase Rate", f"{loyalty['is_repeat'].mean()*100:.1f}%")

with col2:
    st.markdown("### 🔘 Non-Members")
    c1, c2, c3 = st.columns(3)
    c1.metric("Avg CLV", f"${non_loyalty['total_revenue'].mean():,.2f}")
    c2.metric("Avg Order Value", f"${non_loyalty['avg_order_value'].mean():,.2f}")
    c3.metric("Repeat Purchase Rate", f"{non_loyalty['is_repeat'].mean()*100:.1f}%")

st.markdown("---")

# ── Row 1: CLV Distribution Comparison ────────────────────────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("CLV Distribution by Loyalty Status")
    # Cap at 95th percentile so the distribution shape is readable
    plot_df = user_orders[user_orders["total_revenue"] > 0].copy()
    cap = plot_df["total_revenue"].quantile(0.95)
    plot_df = plot_df[plot_df["total_revenue"] <= cap]
    fig_hist = px.histogram(
        plot_df, x="total_revenue", color="loyalty_status",
        nbins=50, barmode="overlay", opacity=0.7,
        template="plotly_dark", color_discrete_map=LOYALTY_COLORS,
        labels={"total_revenue": "Customer Lifetime Value ($)"}
    )
    fig_hist.update_layout(height=400)
    st.plotly_chart(fig_hist, use_container_width=True)
    st.caption(f"_Showing customers up to the 95th percentile (≤ ${cap:,.0f}) for readability._")

with col_b:
    st.subheader("Repeat Purchase Rate Comparison")
    repeat_data = user_orders.groupby("loyalty_status").agg(
        total_customers=("user_id", "count"),
        repeat_customers=("is_repeat", "sum"),
    ).reset_index()
    repeat_data["repeat_rate"] = repeat_data["repeat_customers"] / repeat_data["total_customers"] * 100
    fig_repeat = px.bar(
        repeat_data, x="loyalty_status", y="repeat_rate",
        color="loyalty_status", template="plotly_dark",
        color_discrete_map=LOYALTY_COLORS,
        text=repeat_data["repeat_rate"].round(1).astype(str) + "%",
        labels={"repeat_rate": "Repeat Purchase Rate (%)"}
    )
    fig_repeat.update_layout(height=400, showlegend=False)
    fig_repeat.update_traces(textposition="outside")
    st.plotly_chart(fig_repeat, use_container_width=True)

st.markdown("---")

# ── Row 1b: Box Plot — Full CLV Range Including Top 5% ───────────────────────
st.subheader("Full CLV Range (Including Top 5% Outliers)")
st.caption("The box shows the median and interquartile range (middle 50%). "
           "Individual dots on the right represent the high-value customers "
           "excluded from the histogram above.")

all_positive = user_orders[user_orders["total_revenue"] > 0].copy()
fig_box = px.box(
    all_positive, x="loyalty_status", y="total_revenue",
    color="loyalty_status", points="outliers",
    template="plotly_dark", color_discrete_map=LOYALTY_COLORS,
    labels={"total_revenue": "Customer Lifetime Value ($)", "loyalty_status": ""}
)
fig_box.update_layout(height=400, showlegend=False)
st.plotly_chart(fig_box, use_container_width=True)

st.markdown("---")

# ── Row 2: Monthly Revenue Split ─────────────────────────────────────────────
st.subheader("Monthly Revenue: Loyalty vs Non-Loyalty")
df_orders["loyalty_status"] = df_orders["is_loyalty"].map({True: "Loyalty Member", False: "Non-Member"})
monthly = df_orders.groupby([pd.Grouper(key="date_key", freq="ME"), "loyalty_status"])["net_revenue"].sum().reset_index()
fig_line = px.line(
    monthly, x="date_key", y="net_revenue", color="loyalty_status",
    template="plotly_dark", color_discrete_map=LOYALTY_COLORS,
    labels={"net_revenue": "Net Revenue ($)", "date_key": "Month"}
)
fig_line.update_layout(height=400)
st.plotly_chart(fig_line, use_container_width=True)
