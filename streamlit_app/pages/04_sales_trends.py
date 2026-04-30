import streamlit as st
import pandas as pd
import plotly.express as px
from data_loader import load_daily_sales, load_dim_restaurant

# --- Load CSS ---
with open("streamlit_app/style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.title("📅 Sales Trends & Categories")

df_sales = load_daily_sales()
df_rest = load_dim_restaurant()

# Merge location labels
df_sales = df_sales.merge(df_rest[["restaurant_id", "location_label"]], on="restaurant_id", how="left")

# Filters
st.sidebar.header("Filters")
min_date = df_sales["date_key"].min().date()
max_date = df_sales["date_key"].max().date()
date_range = st.sidebar.date_input("Date Range", [min_date, max_date], min_value=min_date, max_value=max_date)

if len(date_range) != 2:
    st.stop()

mask = (df_sales["date_key"].dt.date >= date_range[0]) & (df_sales["date_key"].dt.date <= date_range[1])
filtered_sales = df_sales[mask]

st.subheader("Revenue by Category")
# Aggregate by category
cat_sales = filtered_sales.groupby("item_category")[["net_revenue", "total_items_sold"]].sum().reset_index()
cat_sales = cat_sales.sort_values("net_revenue", ascending=False)

fig_cat = px.bar(
    cat_sales.head(15), 
    x="net_revenue", 
    y="item_category", 
    orientation="h",
    title="Top 15 Categories by Net Revenue",
    template="plotly_dark",
    color="net_revenue",
    color_continuous_scale="Blues"
)
fig_cat.update_layout(yaxis={'categoryorder':'total ascending'})
st.plotly_chart(fig_cat, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Discounting Impact")
    total_orders = filtered_sales["total_orders"].sum()
    discounted_orders = filtered_sales["discounted_order_count"].sum()
    
    if total_orders > 0:
        disc_pct = (discounted_orders / total_orders) * 100
        st.metric("Orders with Discounts", f"{disc_pct:.1f}%")
        
        disc_pie = pd.DataFrame({
            "Type": ["Discounted", "Full Price"],
            "Count": [discounted_orders, total_orders - discounted_orders]
        })
        fig_pie = px.pie(disc_pie, values="Count", names="Type", hole=0.6, template="plotly_dark", color_discrete_sequence=["#f59e0b", "#4f8ef7"])
        st.plotly_chart(fig_pie, use_container_width=True)

with col2:
    st.subheader("Weekend vs Weekday")
    filtered_sales["is_weekend"] = filtered_sales["date_key"].dt.dayofweek >= 5
    filtered_sales["day_type"] = filtered_sales["is_weekend"].map({True: "Weekend", False: "Weekday"})
    
    day_sales = filtered_sales.groupby("day_type")["net_revenue"].sum().reset_index()
    fig_day = px.bar(day_sales, x="day_type", y="net_revenue", color="day_type", template="plotly_dark", color_discrete_map={"Weekend": "#8b5cf6", "Weekday": "#4f8ef7"})
    st.plotly_chart(fig_day, use_container_width=True)
