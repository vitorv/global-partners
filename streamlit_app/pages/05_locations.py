import streamlit as st
import pandas as pd
import plotly.express as px
from data_loader import load_daily_sales, load_dim_restaurant

# --- Load CSS ---
with open("streamlit_app/style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.title("🏪 Location Performance")

df_sales = load_daily_sales()
df_rest = load_dim_restaurant()

# Merge location labels
df_sales = df_sales.merge(df_rest[["restaurant_id", "location_label"]], on="restaurant_id", how="left")

# Aggregate by location
loc_sales = df_sales.groupby("location_label").agg(
    total_revenue=("net_revenue", "sum"),
    total_orders=("total_orders", "sum"),
).reset_index()

loc_sales["avg_order_value"] = loc_sales["total_revenue"] / loc_sales["total_orders"]
loc_sales = loc_sales.sort_values("total_revenue", ascending=False)

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Location Leaderboard (Net Revenue)")
    fig_bar = px.bar(
        loc_sales, 
        x="total_revenue", 
        y="location_label", 
        orientation="h",
        template="plotly_dark",
        color="total_revenue",
        color_continuous_scale="Blues"
    )
    fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig_bar, use_container_width=True)

with col2:
    st.subheader("Volume vs AOV")
    fig_scatter = px.scatter(
        loc_sales,
        x="total_orders",
        y="avg_order_value",
        hover_name="location_label",
        size="total_revenue",
        color="total_revenue",
        template="plotly_dark",
        color_continuous_scale="Blues"
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

st.subheader("Revenue Over Time by Location")
selected_locs = st.multiselect("Select Locations to Compare", loc_sales["location_label"].tolist(), default=loc_sales["location_label"].head(3).tolist())

if selected_locs:
    trend_sales = df_sales[df_sales["location_label"].isin(selected_locs)]
    # Monthly aggregation
    trend_monthly = trend_sales.groupby([pd.Grouper(key="date_key", freq="ME"), "location_label"])["net_revenue"].sum().reset_index()
    
    fig_line = px.line(trend_monthly, x="date_key", y="net_revenue", color="location_label", template="plotly_dark")
    st.plotly_chart(fig_line, use_container_width=True)
