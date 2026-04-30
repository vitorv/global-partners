import streamlit as st
import pandas as pd
import plotly.express as px
from data_loader import load_customer_rfm

# --- Load CSS ---
with open("streamlit_app/style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.title("🎯 RFM Customer Segmentation")

df_rfm = load_customer_rfm()

# Layout
col1, col2 = st.columns([2, 1])

color_map = {
    "VIP": "#22c55e",
    "Regular": "#8b5cf6",
    "New Customer": "#64748b",
    "Churn Risk": "#ef4444"
}

with col1:
    st.subheader("RFM Scatter (Frequency vs Monetary)")
    # Sample down to 2000 points if too large to avoid lag
    plot_df = df_rfm.sample(n=min(2000, len(df_rfm)), random_state=42).copy()
    
    # Recency inverse for size (so smaller recency = bigger dot)
    plot_df["bubble_size"] = 1 / (plot_df["recency_days"] + 1)
    
    fig_scatter = px.scatter(
        plot_df, 
        x="frequency", 
        y="monetary", 
        color="rfm_segment",
        size="bubble_size",
        hover_data=["user_id", "recency_days"],
        color_discrete_map=color_map,
        template="plotly_dark",
        title="Sample of 2,000 Customers"
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

with col2:
    st.subheader("Segment Breakdown")
    counts = df_rfm["rfm_segment"].value_counts().reset_index()
    fig_bar = px.bar(
        counts, 
        x="count", 
        y="rfm_segment", 
        color="rfm_segment",
        orientation="h",
        color_discrete_map=color_map,
        template="plotly_dark"
    )
    fig_bar.update_layout(showlegend=False)
    st.plotly_chart(fig_bar, use_container_width=True)

st.markdown("---")
st.subheader("Segment Averages")

avg_scores = df_rfm.groupby("rfm_segment")[["recency_days", "frequency", "monetary"]].mean().round(1).reset_index()
st.dataframe(avg_scores, use_container_width=True)
