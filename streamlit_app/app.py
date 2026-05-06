import os
import streamlit as st

st.set_page_config(
    page_title="Global Partners Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Get absolute directory of this script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Load custom CSS
with open(os.path.join(current_dir, "style.css")) as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Use the new multipage navigation API (Streamlit >= 1.36.0)
pg = st.navigation([
    st.Page(os.path.join(current_dir, "pages", "01_customer_segmentation.py"), title="Customer Segmentation"),
    st.Page(os.path.join(current_dir, "pages", "02_churn_risk.py"), title="Churn Risk Indicators"),
    st.Page(os.path.join(current_dir, "pages", "03_sales_trends.py"), title="Sales Trends & Seasonality"),
    st.Page(os.path.join(current_dir, "pages", "04_loyalty_impact.py"), title="Loyalty Program Impact"),
    st.Page(os.path.join(current_dir, "pages", "05_location_performance.py"), title="Location Performance"),
    st.Page(os.path.join(current_dir, "pages", "06_pricing_discounts.py"), title="Pricing & Discounts"),
])

st.sidebar.title("Global Partners")
st.sidebar.markdown("---")
st.sidebar.caption("Analytics Dashboard — Step 6 Deliverables")

pg.run()
