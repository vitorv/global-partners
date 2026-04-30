"""
Location Performance Dashboard
PDF Question: Which restaurant locations generate the highest revenue, and what
operational metrics (e.g., average order size, customer retention) distinguish
top performers?
Focus: Rank locations and highlight actionable insights for expansion or
targeted improvements.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from data_loader import load_order_summary, load_dim_restaurant, load_daily_sales

st.title("Location Performance Dashboard")
st.caption("**Question:** Which restaurant locations generate the highest revenue, "
           "and what operational metrics distinguish top performers?")

# ── Load Data ────────────────────────────────────────────────────────────────
df_orders = load_order_summary()
df_rest = load_dim_restaurant()
df_daily = load_daily_sales()

# Map location labels
rest_map = df_rest.set_index("restaurant_id")["location_label"].to_dict()
df_orders["location_label"] = df_orders["restaurant_id"].map(rest_map)

# ── Aggregate per location ───────────────────────────────────────────────────
loc_agg = df_orders.groupby("location_label").agg(
    total_revenue=("net_revenue", "sum"),
    total_orders=("order_id", "count"),
    unique_customers=("user_id", "nunique"),
    avg_order_value=("net_revenue", "mean"),
).reset_index()

# Customer retention: % of customers who ordered more than once at this location
user_loc_counts = df_orders.groupby(["location_label", "user_id"]).size().reset_index(name="order_count")
repeat_users = user_loc_counts[user_loc_counts["order_count"] > 1].groupby("location_label")["user_id"].count().reset_index(name="repeat_customers")
all_users = user_loc_counts.groupby("location_label")["user_id"].count().reset_index(name="total_customers")
retention = all_users.merge(repeat_users, on="location_label", how="left").fillna(0)
retention["retention_rate"] = (retention["repeat_customers"] / retention["total_customers"] * 100).round(1)

loc_agg = loc_agg.merge(retention[["location_label", "retention_rate"]], on="location_label", how="left")
loc_agg = loc_agg.sort_values("total_revenue", ascending=False)

# ── KPI Cards ────────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)
col1.metric("Total Locations", f"{len(loc_agg)}")
col2.metric("Highest Revenue Location", f"${loc_agg['total_revenue'].max():,.0f}")
col3.metric("Avg Retention Rate", f"{loc_agg['retention_rate'].mean():.1f}%")

st.markdown("---")

# ── Row 1: Leaderboard + Scatter ─────────────────────────────────────────────
col_a, col_b = st.columns([2, 1])

with col_a:
    st.subheader("Location Revenue Leaderboard")
    fig_bar = px.bar(
        loc_agg, x="total_revenue", y="location_label", orientation="h",
        template="plotly_dark", color="total_revenue",
        color_continuous_scale="Blues"
    )
    fig_bar.update_layout(height=600, yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_bar, use_container_width=True)

with col_b:
    st.subheader("Volume vs AOV")
    fig_scatter = px.scatter(
        loc_agg, x="total_orders", y="avg_order_value",
        size="total_revenue", hover_name="location_label",
        color="retention_rate", template="plotly_dark",
        color_continuous_scale="Greens",
        labels={"total_orders": "Total Orders", "avg_order_value": "AOV ($)",
                "retention_rate": "Retention %"}
    )
    fig_scatter.update_layout(height=600)
    st.plotly_chart(fig_scatter, use_container_width=True)

st.markdown("---")

# ── Row 2: Retention Bar Chart ────────────────────────────────────────────────
st.subheader("Customer Retention Rate by Location")
st.caption("Percentage of customers who placed more than one order at this location.")
fig_ret = px.bar(
    loc_agg.sort_values("retention_rate", ascending=False),
    x="retention_rate", y="location_label", orientation="h",
    template="plotly_dark", color="retention_rate",
    color_continuous_scale="RdYlGn",
    text=loc_agg.sort_values("retention_rate", ascending=False)["retention_rate"].astype(str) + "%"
)
fig_ret.update_layout(height=600, yaxis={"categoryorder": "total ascending"})
fig_ret.update_traces(textposition="outside")
st.plotly_chart(fig_ret, use_container_width=True)

st.markdown("---")

# ── Row 3: Location Comparison Over Time ──────────────────────────────────────
st.subheader("Location Revenue Over Time")
df_daily = df_daily.merge(df_rest[["restaurant_id", "location_label"]], on="restaurant_id", how="left")
top_3 = loc_agg["location_label"].head(3).tolist()
selected_locs = st.multiselect("Select Locations to Compare", loc_agg["location_label"].tolist(), default=top_3)

if selected_locs:
    trend = df_daily[df_daily["location_label"].isin(selected_locs)]
    monthly = trend.groupby([pd.Grouper(key="date_key", freq="ME"), "location_label"])["net_revenue"].sum().reset_index()
    fig_line = px.line(
        monthly, x="date_key", y="net_revenue", color="location_label",
        template="plotly_dark"
    )
    fig_line.update_layout(height=400)
    st.plotly_chart(fig_line, use_container_width=True)

# ── Full Ranking Table ────────────────────────────────────────────────────────
st.subheader("Full Location Ranking")
display_df = loc_agg.rename(columns={
    "location_label": "Location",
    "total_revenue": "Net Revenue ($)",
    "total_orders": "Orders",
    "unique_customers": "Unique Customers",
    "avg_order_value": "AOV ($)",
    "retention_rate": "Retention Rate (%)"
})
display_df["Net Revenue ($)"] = display_df["Net Revenue ($)"].round(2)
display_df["AOV ($)"] = display_df["AOV ($)"].round(2)
st.dataframe(display_df, use_container_width=True, hide_index=True)
