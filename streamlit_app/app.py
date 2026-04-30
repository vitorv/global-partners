import streamlit as st

st.set_page_config(
    page_title="Global Partners Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load custom CSS
with open("streamlit_app/style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Use the new multipage navigation API (Streamlit >= 1.36.0)
pg = st.navigation([
    st.Page("pages/01_customer_segmentation.py", title="Customer Segmentation"),
    st.Page("pages/02_churn_risk.py", title="Churn Risk Indicators"),
    st.Page("pages/03_sales_trends.py", title="Sales Trends & Seasonality"),
    st.Page("pages/04_loyalty_impact.py", title="Loyalty Program Impact"),
    st.Page("pages/05_location_performance.py", title="Location Performance"),
    st.Page("pages/06_pricing_discounts.py", title="Pricing & Discounts"),
])

st.sidebar.title("Global Partners")
st.sidebar.markdown("---")
st.sidebar.caption("Analytics Dashboard — Step 6 Deliverables")

pg.run()
