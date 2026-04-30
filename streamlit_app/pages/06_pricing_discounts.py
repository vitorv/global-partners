"""
Pricing and Discount Effectiveness Dashboard
PDF Question: How are discounts and promotions affecting overall sales volume
and net revenue?
Focus: Compare revenue and profit (gross versus net after discount adjustments)
for discounted versus full-price transactions to refine pricing strategies.

Note: The raw data does not contain discount records (no negative option_price
values exist). Instead, we analyze the pricing impact of add-on options (orders
with paid add-ons vs orders without) which is the closest available proxy.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from data_loader import load_order_summary

st.title("Pricing & Discount Effectiveness Dashboard")
st.caption("**Question:** How are discounts and promotions affecting overall "
           "sales volume and net revenue?")

# ── Load Data ────────────────────────────────────────────────────────────────
df_orders = load_order_summary()

# Since no negative option prices exist in the data, we analyze
# orders with paid add-ons vs orders without add-ons
df_orders["order_type"] = df_orders["option_revenue"].apply(
    lambda x: "With Paid Add-ons" if x > 0 else "Base Order Only"
)

TYPE_COLORS = {"With Paid Add-ons": "#f59e0b", "Base Order Only": "#4f8ef7"}

# ── Aggregate by order type ──────────────────────────────────────────────────
type_agg = df_orders.groupby("order_type").agg(
    order_count=("order_id", "count"),
    total_gross=("gross_revenue", "sum"),
    total_net=("net_revenue", "sum"),
    total_option_rev=("option_revenue", "sum"),
    avg_gross=("gross_revenue", "mean"),
    avg_net=("net_revenue", "mean"),
    avg_option_rev=("option_revenue", "mean"),
).reset_index()

# ── KPI Cards ────────────────────────────────────────────────────────────────
addon = type_agg[type_agg["order_type"] == "With Paid Add-ons"]
base = type_agg[type_agg["order_type"] == "Base Order Only"]

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🟡 Orders With Paid Add-ons")
    c1, c2, c3 = st.columns(3)
    if not addon.empty:
        c1.metric("Orders", f"{addon['order_count'].values[0]:,}")
        c2.metric("Avg Gross Rev", f"${addon['avg_gross'].values[0]:,.2f}")
        c3.metric("Avg Net Rev", f"${addon['avg_net'].values[0]:,.2f}")
    else:
        c1.metric("Orders", "0")
        c2.metric("Avg Gross Rev", "$0.00")
        c3.metric("Avg Net Rev", "$0.00")

with col2:
    st.markdown("### 🔵 Base Orders Only")
    c1, c2, c3 = st.columns(3)
    if not base.empty:
        c1.metric("Orders", f"{base['order_count'].values[0]:,}")
        c2.metric("Avg Gross Rev", f"${base['avg_gross'].values[0]:,.2f}")
        c3.metric("Avg Net Rev", f"${base['avg_net'].values[0]:,.2f}")
    else:
        c1.metric("Orders", "0")
        c2.metric("Avg Gross Rev", "$0.00")
        c3.metric("Avg Net Rev", "$0.00")

st.markdown("---")

# ── Row 1: Gross vs Net Comparison + Revenue Waterfall ───────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Gross vs Net Revenue by Order Type")
    fig_grouped = go.Figure()
    for ot, color in TYPE_COLORS.items():
        row = type_agg[type_agg["order_type"] == ot]
        if not row.empty:
            fig_grouped.add_trace(go.Bar(
                name=ot,
                x=["Gross Revenue", "Net Revenue"],
                y=[row["total_gross"].values[0], row["total_net"].values[0]],
                marker_color=color
            ))
    fig_grouped.update_layout(barmode="group", template="plotly_dark", height=400)
    st.plotly_chart(fig_grouped, use_container_width=True)

with col_b:
    st.subheader("Revenue Composition")
    st.caption("How base item revenue and add-on revenue compose total gross.")
    total_gross = df_orders["gross_revenue"].sum()
    total_option = df_orders["option_revenue"].sum()
    total_base_item = total_gross - total_option

    fig_waterfall = go.Figure(go.Waterfall(
        x=["Base Item Revenue", "Add-on Revenue", "Total Gross"],
        y=[total_base_item, total_option, total_gross],
        measure=["absolute", "relative", "total"],
        textposition="outside",
        text=[f"${total_base_item:,.0f}", f"+${total_option:,.0f}", f"${total_gross:,.0f}"],
        connector={"line": {"color": "rgba(255,255,255,0.3)"}},
        increasing={"marker": {"color": "#22c55e"}},
        totals={"marker": {"color": "#4f8ef7"}}
    ))
    fig_waterfall.update_layout(template="plotly_dark", height=400, showlegend=False)
    st.plotly_chart(fig_waterfall, use_container_width=True)

st.markdown("---")

# ── Row 2: Add-on Rate Over Time ─────────────────────────────────────────────
st.subheader("Add-on Attachment Rate Over Time")
st.caption("Percentage of orders that included a paid add-on each month.")

monthly = df_orders.groupby(pd.Grouper(key="date_key", freq="ME")).agg(
    total_orders=("order_id", "count"),
    addon_orders=("option_revenue", lambda x: (x > 0).sum()),
).reset_index()
monthly["addon_rate"] = (monthly["addon_orders"] / monthly["total_orders"] * 100).round(1)

fig_rate = px.line(
    monthly, x="date_key", y="addon_rate",
    template="plotly_dark", color_discrete_sequence=["#f59e0b"],
    labels={"addon_rate": "Add-on Rate (%)", "date_key": "Month"}
)
fig_rate.update_layout(height=350)
st.plotly_chart(fig_rate, use_container_width=True)

st.markdown("---")

# ── Row 3: Volume Split + Average Add-on Value ───────────────────────────────
col_c, col_d = st.columns(2)

with col_c:
    st.subheader("Order Volume Split")
    order_counts = df_orders["order_type"].value_counts().reset_index()
    fig_pie = px.pie(
        order_counts, values="count", names="order_type", hole=0.5,
        color="order_type", color_discrete_map=TYPE_COLORS,
        template="plotly_dark"
    )
    fig_pie.update_layout(height=350)
    st.plotly_chart(fig_pie, use_container_width=True)

with col_d:
    st.subheader("Add-on Revenue Insights")
    addon_orders = df_orders[df_orders["option_revenue"] > 0]
    if not addon_orders.empty:
        avg_addon = addon_orders["option_revenue"].mean()
        total_addon = addon_orders["option_revenue"].sum()
        addon_pct = total_addon / total_gross * 100 if total_gross > 0 else 0

        st.metric("Avg Add-on Revenue per Order", f"${avg_addon:,.2f}")
        st.metric("Total Add-on Revenue", f"${total_addon:,.0f}")
        st.metric("Add-ons as % of Gross Revenue", f"{addon_pct:.1f}%")
        st.caption("This shows the revenue uplift generated by optional add-on "
                   "items like extra toppings, substitutions, and premium ingredients.")
    else:
        st.info("No orders with paid add-ons found in the data.")
