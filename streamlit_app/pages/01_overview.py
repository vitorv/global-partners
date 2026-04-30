import streamlit as st
import pandas as pd
import plotly.express as px
from data_loader import load_order_summary, load_customer_rfm, load_customer_daily, load_dim_restaurant

# --- Load CSS ---
with open("streamlit_app/style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.title("📊 Executive Overview")

# --- Load Data ---
df_orders = load_order_summary()
df_rfm = load_customer_rfm()
df_daily_snap = load_customer_daily()
df_rest = load_dim_restaurant()

# --- Sidebar Filters ---
st.sidebar.header("Filters")

# Date range filter
min_date = df_orders["date_key"].min().date()
max_date = df_orders["date_key"].max().date()
date_range = st.sidebar.date_input("Date Range", [min_date, max_date], min_value=min_date, max_value=max_date)

# Restaurant filter
all_locations = sorted(df_rest["location_label"].tolist())
selected_locations = st.sidebar.multiselect("Restaurants", all_locations, default=all_locations)

if len(date_range) != 2:
    st.stop()
    
start_date, end_date = date_range

# Map selected locations back to IDs
selected_ids = df_rest[df_rest["location_label"].isin(selected_locations)]["restaurant_id"].tolist()

# --- Apply Filters ---
mask = (
    (df_orders["date_key"].dt.date >= start_date) & 
    (df_orders["date_key"].dt.date <= end_date) &
    (df_orders["restaurant_id"].isin(selected_ids))
)
filtered_orders = df_orders[mask]

# For snapshots, we just use the latest date in the range
latest_snap = df_daily_snap[df_daily_snap["date_key"].dt.date == end_date]

# --- Calculate KPIs ---
total_revenue = filtered_orders["net_revenue"].sum()
total_orders = len(filtered_orders)
aov = total_revenue / total_orders if total_orders > 0 else 0
active_customers = len(latest_snap[latest_snap["churn_risk_flag"] == "Active"])

# --- Display KPIs ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Net Revenue", f"${total_revenue:,.0f}")
col2.metric("Total Orders", f"{total_orders:,}")
col3.metric("Active Customers", f"{active_customers:,}")
col4.metric("Avg Order Value", f"${aov:,.2f}")

st.markdown("<br>", unsafe_allow_html=True)

# --- Charts ---
st.subheader("Revenue Trends")
# Monthly aggregation
monthly_rev = filtered_orders.set_index("date_key").resample("ME")["net_revenue"].sum().reset_index()
fig_rev = px.line(monthly_rev, x="date_key", y="net_revenue", title="Monthly Net Revenue", template="plotly_dark", color_discrete_sequence=["#4f8ef7"])
st.plotly_chart(fig_rev, use_container_width=True)

col_a, col_b = st.columns(2)

with col_a:
    st.subheader("RFM Segments (Latest)")
    rfm_counts = df_rfm["rfm_segment"].value_counts().reset_index()
    # Apply custom colors
    color_map = {
        "VIP": "#22c55e",
        "Regular": "#8b5cf6",
        "New Customer": "#64748b",
        "Churn Risk": "#ef4444"
    }
    fig_rfm = px.pie(rfm_counts, values="count", names="rfm_segment", hole=0.5, template="plotly_dark", color="rfm_segment", color_discrete_map=color_map)
    st.plotly_chart(fig_rfm, use_container_width=True)

with col_b:
    st.subheader("CLV Segments (Latest)")
    clv_counts = latest_snap["clv_segment"].value_counts().reset_index()
    color_map_clv = {
        "High": "#22c55e",
        "Medium": "#8b5cf6",
        "Low": "#64748b"
    }
    fig_clv = px.pie(clv_counts, values="count", names="clv_segment", hole=0.5, template="plotly_dark", color="clv_segment", color_discrete_map=color_map_clv)
    st.plotly_chart(fig_clv, use_container_width=True)
