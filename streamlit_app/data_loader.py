import pandas as pd
import streamlit as st
import os

# Base directory for Gold Parquet files
BASE_PATH = os.path.join("data", "output", "gold")

@st.cache_data
def load_dim_restaurant():
    df = pd.read_parquet(os.path.join(BASE_PATH, "dim_restaurant"))
    # Create a clean label "Alltown Fresh (#abc123)"
    df["location_label"] = df["app_name"] + " (#" + df["restaurant_id"].str[-6:] + ")"
    return df

@st.cache_data
def load_dim_customer():
    df = pd.read_parquet(os.path.join(BASE_PATH, "dim_customer"))
    df["first_order_date"] = pd.to_datetime(df["first_order_date"])
    df["last_order_date"] = pd.to_datetime(df["last_order_date"])
    return df

@st.cache_data
def load_order_summary():
    df = pd.read_parquet(os.path.join(BASE_PATH, "fct_order_summary"))
    df["date_key"] = pd.to_datetime(df["date_key"].astype(str))
    
    # Cast Decimal objects to float
    for col in ["gross_revenue", "option_revenue", "discount_amount", "net_revenue"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df

@st.cache_data
def load_daily_sales():
    df = pd.read_parquet(os.path.join(BASE_PATH, "fct_daily_sales_summary"))
    df["date_key"] = pd.to_datetime(df["date_key"].astype(str))
    
    # Cast Decimal objects to float
    for col in ["gross_revenue", "net_revenue", "discount_amount", "avg_order_value"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df

@st.cache_data
def load_customer_daily():
    df = pd.read_parquet(os.path.join(BASE_PATH, "fct_customer_daily_snapshot"))
    df["date_key"] = pd.to_datetime(df["date_key"].astype(str))
    
    # Cast Decimal objects to float
    for col in ["daily_net_revenue", "cumulative_ltv", "avg_order_gap_days"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df

@st.cache_data
def load_customer_rfm():
    df = pd.read_parquet(os.path.join(BASE_PATH, "fct_customer_rfm"))
    df["monetary"] = pd.to_numeric(df["monetary"], errors="coerce").fillna(0.0)
    return df
