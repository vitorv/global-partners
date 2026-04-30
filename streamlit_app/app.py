import streamlit as st

st.set_page_config(
    page_title="Global Partners Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Use the new multipage navigation API (Streamlit >= 1.36.0)
pg = st.navigation([
    st.Page("pages/01_overview.py", title="Overview", icon="📈"),
    st.Page("pages/02_customer_ltv.py", title="Customer LTV", icon="💰"),
    st.Page("pages/03_rfm.py", title="RFM Segmentation", icon="🎯"),
    st.Page("pages/04_sales_trends.py", title="Sales Trends", icon="📅"),
    st.Page("pages/05_locations.py", title="Locations", icon="🏪"),
    st.Page("pages/06_churn.py", title="Churn Analysis", icon="⚠️"),
])

st.sidebar.title("Global Partners")
st.sidebar.markdown("---")
st.sidebar.info("Use the navigation menu above to explore different analytics dashboards.")

pg.run()
