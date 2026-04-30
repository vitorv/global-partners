import streamlit as st
import pandas as pd
import plotly.express as px
from data_loader import load_customer_daily, load_dim_customer

# --- Load CSS ---
with open("streamlit_app/style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.title("💰 Customer Lifetime Value (CLV)")

df_daily_snap = load_customer_daily()
df_cust = load_dim_customer()

latest_date = df_daily_snap["date_key"].max()
latest_snap = df_daily_snap[df_daily_snap["date_key"] == latest_date]

tab1, tab2 = st.tabs(["Individual Search", "Segment Analysis"])

with tab1:
    st.subheader("Customer LTV Growth Curve")
    user_id_search = st.text_input("Enter User ID to search", placeholder="e.g. 5eac88d6902ad598127b240b")
    
    if user_id_search:
        user_data = df_daily_snap[df_daily_snap["user_id"] == user_id_search]
        if not user_data.empty:
            st.success(f"Found {len(user_data)} days of history for this user.")
            fig = px.line(user_data, x="date_key", y="cumulative_ltv", title=f"LTV Curve for {user_id_search}", template="plotly_dark", color_discrete_sequence=["#22c55e"])
            st.plotly_chart(fig, use_container_width=True)
            
            st.dataframe(user_data[["date_key", "daily_net_revenue", "cumulative_ltv", "clv_segment", "churn_risk_flag"]].tail(10))
        else:
            st.error("User ID not found.")
            
    st.subheader("Top 20 Customers by LTV")
    top_20 = latest_snap.nlargest(20, "cumulative_ltv")
    fig_top = px.bar(top_20, x="user_id", y="cumulative_ltv", title="Top 20 Customers", template="plotly_dark", color="clv_segment", color_discrete_map={"High": "#22c55e", "Medium": "#8b5cf6", "Low": "#64748b"})
    fig_top.update_xaxes(showticklabels=False) # Hide long user IDs
    st.plotly_chart(fig_top, use_container_width=True)

with tab2:
    st.subheader("LTV Distribution (All Customers)")
    # Filter out 0 LTV for the histogram to make it readable
    ltv_dist = latest_snap[latest_snap["cumulative_ltv"] > 0]
    fig_hist = px.histogram(ltv_dist, x="cumulative_ltv", nbins=50, title="Distribution of LTV (>$0)", template="plotly_dark", color_discrete_sequence=["#4f8ef7"])
    st.plotly_chart(fig_hist, use_container_width=True)
    
    st.subheader("CLV Segments Over Time")
    # Group by date and segment
    segment_trends = df_daily_snap.groupby(["date_key", "clv_segment"]).size().reset_index(name="count")
    color_map = {"High": "#22c55e", "Medium": "#8b5cf6", "Low": "#64748b"}
    fig_area = px.area(segment_trends, x="date_key", y="count", color="clv_segment", title="Segment Growth Over Time", template="plotly_dark", color_discrete_map=color_map)
    st.plotly_chart(fig_area, use_container_width=True)
