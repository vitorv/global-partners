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

# Merge location labels and date attributes
df_sales = df_sales.merge(df_rest[["restaurant_id", "location_label"]], on="restaurant_id", how="left")
df_sales = df_sales.merge(
    df_dates[["date_key", "is_holiday", "holiday_name", "month_name", "month_number", "year"]],
    on="date_key", how="left"
)

# Derive season from month_number
def get_season(m):
    if m in (12, 1, 2):
        return "Winter"
    elif m in (3, 4, 5):
        return "Spring"
    elif m in (6, 7, 8):
        return "Summer"
    else:
        return "Fall"

df_sales["season"] = df_sales["month_number"].apply(get_season)

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

# Pre-compute top categories and locations for multi-line charts
top_categories = (
    filtered.groupby("item_category")["net_revenue"].sum()
    .sort_values(ascending=False).head(8).index.tolist()
)
top_locations = (
    filtered.groupby("location_label")["net_revenue"].sum()
    .sort_values(ascending=False).head(5).index.tolist()
)

# ── KPI Cards ────────────────────────────────────────────────────────────────
total_rev = filtered["net_revenue"].sum()
total_orders = filtered["total_orders"].sum()
aov = total_rev / total_orders if total_orders > 0 else 0

col1, col2, col3 = st.columns(3)
col1.metric("Total Net Revenue", f"${total_rev:,.0f}")
col2.metric("Total Orders", f"{total_orders:,}")
col3.metric("Avg Order Value", f"${aov:,.2f}")

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: MONTHLY TRENDS
# ═══════════════════════════════════════════════════════════════════════════════
st.header("📈 Monthly Trends")

# ── 1a. Monthly Revenue by Product Category ──────────────────────────────────
st.subheader("Monthly Revenue by Product Category")
st.caption("Top 8 categories shown. Use the legend to toggle categories on/off.")

cat_monthly = (
    filtered[filtered["item_category"].isin(top_categories)]
    .groupby([pd.Grouper(key="date_key", freq="ME"), "item_category"])["net_revenue"]
    .sum().reset_index()
)
fig_cat_monthly = px.line(
    cat_monthly, x="date_key", y="net_revenue", color="item_category",
    template="plotly_dark",
    labels={"net_revenue": "Net Revenue ($)", "date_key": "Month", "item_category": "Category"}
)
fig_cat_monthly.update_layout(height=400)
st.plotly_chart(fig_cat_monthly, use_container_width=True)

st.markdown("---")

# ── 1b. Monthly Revenue by Location ──────────────────────────────────────────
st.subheader("Monthly Revenue by Location")
st.caption("Top 5 locations by total revenue shown. Select others below.")

selected_locs_monthly = st.multiselect(
    "Select locations for monthly trend",
    filtered["location_label"].unique().tolist(),
    default=top_locations,
    key="monthly_loc"
)

if selected_locs_monthly:
    loc_monthly = (
        filtered[filtered["location_label"].isin(selected_locs_monthly)]
        .groupby([pd.Grouper(key="date_key", freq="ME"), "location_label"])["net_revenue"]
        .sum().reset_index()
    )
    fig_loc_monthly = px.line(
        loc_monthly, x="date_key", y="net_revenue", color="location_label",
        template="plotly_dark",
        labels={"net_revenue": "Net Revenue ($)", "date_key": "Month", "location_label": "Location"}
    )
    fig_loc_monthly.update_layout(height=400)
    st.plotly_chart(fig_loc_monthly, use_container_width=True)

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: SEASONAL TRENDS
# ═══════════════════════════════════════════════════════════════════════════════
st.header("🍂 Seasonal Trends")

SEASON_ORDER = ["Winter", "Spring", "Summer", "Fall"]
SEASON_COLORS = {
    "Winter": "#60a5fa",
    "Spring": "#34d399",
    "Summer": "#fbbf24",
    "Fall": "#f97316"
}

# ── 2a. Seasonal Revenue by Product Category ─────────────────────────────────
st.subheader("Seasonal Revenue by Product Category")
st.caption("Average daily revenue per season for the top 8 categories. "
           "Shows which categories perform best in each season.")

cat_seasonal = (
    filtered[filtered["item_category"].isin(top_categories)]
    .groupby(["season", "item_category"])
    .agg(total_rev=("net_revenue", "sum"), days=("date_key", "nunique"))
    .reset_index()
)
cat_seasonal["avg_daily_revenue"] = cat_seasonal["total_rev"] / cat_seasonal["days"]

fig_cat_season = px.bar(
    cat_seasonal, x="item_category", y="avg_daily_revenue",
    color="season", barmode="group",
    template="plotly_dark",
    color_discrete_map=SEASON_COLORS,
    category_orders={"season": SEASON_ORDER},
    labels={"avg_daily_revenue": "Avg Daily Revenue ($)", "item_category": "Category"}
)
fig_cat_season.update_layout(height=450)
st.plotly_chart(fig_cat_season, use_container_width=True)

st.markdown("---")

# ── 2b. Seasonal Revenue by Location ─────────────────────────────────────────
st.subheader("Seasonal Revenue by Location")
st.caption("Average daily revenue per season for each location. "
           "Identifies which locations have seasonal peaks or dips.")

loc_seasonal = (
    filtered
    .groupby(["season", "location_label"])
    .agg(total_rev=("net_revenue", "sum"), days=("date_key", "nunique"))
    .reset_index()
)
loc_seasonal["avg_daily_revenue"] = loc_seasonal["total_rev"] / loc_seasonal["days"]

# Show top 10 locations by total revenue for readability
top_locs_seasonal = (
    loc_seasonal.groupby("location_label")["total_rev"].sum()
    .sort_values(ascending=False).head(10).index.tolist()
)
loc_seasonal_top = loc_seasonal[loc_seasonal["location_label"].isin(top_locs_seasonal)]

fig_loc_season = px.bar(
    loc_seasonal_top, x="location_label", y="avg_daily_revenue",
    color="season", barmode="group",
    template="plotly_dark",
    color_discrete_map=SEASON_COLORS,
    category_orders={"season": SEASON_ORDER},
    labels={"avg_daily_revenue": "Avg Daily Revenue ($)", "location_label": "Location"}
)
fig_loc_season.update_layout(height=450, xaxis_tickangle=-45)
st.plotly_chart(fig_loc_season, use_container_width=True)

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: SUPPORTING DETAIL
# ═══════════════════════════════════════════════════════════════════════════════
st.header("📊 Supporting Detail")

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
    st.subheader("Revenue by Season (Overall)")
    season_total = (
        filtered.groupby("season")
        .agg(total_rev=("net_revenue", "sum"), days=("date_key", "nunique"))
        .reset_index()
    )
    season_total["avg_daily_revenue"] = season_total["total_rev"] / season_total["days"]
    fig_season_overall = px.bar(
        season_total, x="season", y="avg_daily_revenue",
        color="season", template="plotly_dark",
        color_discrete_map=SEASON_COLORS,
        category_orders={"season": SEASON_ORDER},
        labels={"avg_daily_revenue": "Avg Daily Revenue ($)"}
    )
    fig_season_overall.update_layout(height=350, showlegend=False)
    st.plotly_chart(fig_season_overall, use_container_width=True)
