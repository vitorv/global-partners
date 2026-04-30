"""
Sales Trends and Seasonality Dashboard
PDF Question: What are the monthly and seasonal trends in sales, and how do
these vary by product category or location?
Focus: Track weekly/monthly sales aggregates and holiday spikes to support
inventory planning and staffing decisions.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from data_loader import load_daily_sales, load_dim_restaurant, load_dim_date

st.title("Sales Trends & Seasonality Dashboard")
st.caption("**Question:** What are the monthly and seasonal trends in sales, "
           "and how do these vary by product category or location?")

# ── Load Data ────────────────────────────────────────────────────────────────
df_sales = load_daily_sales()
df_rest = load_dim_restaurant()
df_dates = load_dim_date()

# Merge location labels
df_sales = df_sales.merge(df_rest[["restaurant_id", "location_label"]], on="restaurant_id", how="left")
# Merge holiday info
df_sales = df_sales.merge(df_dates[["date_key", "is_holiday", "holiday_name", "month_name"]], on="date_key", how="left")

# ── Sidebar Filters ──────────────────────────────────────────────────────────
st.sidebar.header("Sales Filters")
min_date = df_sales["date_key"].min().date()
max_date = df_sales["date_key"].max().date()
date_range = st.sidebar.date_input("Date Range", [min_date, max_date],
                                    min_value=min_date, max_value=max_date)
if len(date_range) != 2:
    st.stop()

mask = (df_sales["date_key"].dt.date >= date_range[0]) & (df_sales["date_key"].dt.date <= date_range[1])
filtered = df_sales[mask]

# ── KPI Cards ────────────────────────────────────────────────────────────────
total_rev = filtered["net_revenue"].sum()
total_orders = filtered["total_orders"].sum()
aov = total_rev / total_orders if total_orders > 0 else 0

col1, col2, col3 = st.columns(3)
col1.metric("Total Net Revenue", f"${total_rev:,.0f}")
col2.metric("Total Orders", f"{total_orders:,}")
col3.metric("Avg Order Value", f"${aov:,.2f}")

st.markdown("---")

# ── Row 1: Monthly/Weekly Revenue Trend ───────────────────────────────────────
st.subheader("Revenue Over Time")
freq = st.radio("Aggregation", ["Monthly", "Weekly"], horizontal=True)
freq_code = "ME" if freq == "Monthly" else "W"

trend = filtered.groupby(pd.Grouper(key="date_key", freq=freq_code))["net_revenue"].sum().reset_index()
fig_trend = px.line(
    trend, x="date_key", y="net_revenue",
    template="plotly_dark", color_discrete_sequence=["#4f8ef7"],
    title=f"{freq} Net Revenue"
)
fig_trend.update_layout(height=350)
st.plotly_chart(fig_trend, use_container_width=True)

st.markdown("---")

# ── Row 2: Holiday vs Non-Holiday + Category Breakdown ───────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Holiday vs Non-Holiday Sales")
    st.caption("Average daily revenue on holidays compared to regular days.")
    daily_rev = filtered.groupby(["date_key", "is_holiday"])["net_revenue"].sum().reset_index()
    holiday_avg = daily_rev.groupby("is_holiday")["net_revenue"].mean().reset_index()
    holiday_avg["label"] = holiday_avg["is_holiday"].map({True: "Holiday", False: "Non-Holiday"})
    fig_holiday = px.bar(
        holiday_avg, x="label", y="net_revenue", color="label",
        template="plotly_dark",
        color_discrete_map={"Holiday": "#f59e0b", "Non-Holiday": "#4f8ef7"}
    )
    fig_holiday.update_layout(height=350, showlegend=False,
                              yaxis_title="Avg Daily Revenue ($)")
    st.plotly_chart(fig_holiday, use_container_width=True)

with col_b:
    st.subheader("Top 15 Categories by Revenue")
    cat_sales = filtered.groupby("item_category")["net_revenue"].sum().reset_index()
    cat_sales = cat_sales.sort_values("net_revenue", ascending=False).head(15)
    fig_cat = px.bar(
        cat_sales, x="net_revenue", y="item_category", orientation="h",
        template="plotly_dark", color="net_revenue",
        color_continuous_scale="Blues"
    )
    fig_cat.update_layout(height=350, yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_cat, use_container_width=True)

st.markdown("---")

# ── Row 3: Revenue by Location ───────────────────────────────────────────────
st.subheader("Revenue by Location")
loc_rev = filtered.groupby("location_label")["net_revenue"].sum().reset_index()
loc_rev = loc_rev.sort_values("net_revenue", ascending=False)
fig_loc = px.bar(
    loc_rev, x="net_revenue", y="location_label", orientation="h",
    template="plotly_dark", color="net_revenue",
    color_continuous_scale="Greens"
)
fig_loc.update_layout(height=500, yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig_loc, use_container_width=True)
