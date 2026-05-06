import pandas as pd
import streamlit as st
import os

# ── Path resolver: local dev vs AWS production ────────────────────────────────
# Set PIPELINE_ENV=aws and S3_BUCKET=<bucket-name> in the App Runner environment
# variables to switch from local Parquet reads to S3 reads.
# pandas.read_parquet() uses s3fs transparently when given an s3:// path.
_PIPELINE_ENV = os.environ.get("PIPELINE_ENV", "local")
_S3_BUCKET = os.environ.get("S3_BUCKET", "")

if _PIPELINE_ENV == "aws":
    BASE_PATH = f"s3://{_S3_BUCKET}/gold"
    S3_OPTS = {"client_kwargs": {"region_name": "us-east-1"}}
else:
    BASE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "output", "gold")
    S3_OPTS = None

@st.cache_data(ttl=3600, show_spinner=False)
def load_dim_restaurant():
    df = pd.read_parquet(os.path.join(BASE_PATH, "dim_restaurant"), storage_options=S3_OPTS)
    # Create a clean label "Alltown Fresh (#abc123)"
    df["location_label"] = df["app_name"] + " (#" + df["restaurant_id"].str[-6:] + ")"
    return df

@st.cache_data(ttl=3600, show_spinner=False)
def load_dim_customer():
    df = pd.read_parquet(os.path.join(BASE_PATH, "dim_customer"), storage_options=S3_OPTS)
    df["first_order_date"] = pd.to_datetime(df["first_order_date"])
    df["last_order_date"] = pd.to_datetime(df["last_order_date"])
    return df

@st.cache_data(ttl=3600, show_spinner=False)
def load_order_summary():
    df = pd.read_parquet(os.path.join(BASE_PATH, "fct_order_summary"), storage_options=S3_OPTS)
    df["date_key"] = pd.to_datetime(df["date_key"].astype(str))
    
    # Cast Decimal objects to float
    for col in ["gross_revenue", "option_revenue", "discount_amount", "net_revenue"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df

@st.cache_data(ttl=3600, show_spinner=False)
def load_daily_sales():
    df = pd.read_parquet(os.path.join(BASE_PATH, "fct_daily_sales_summary"), storage_options=S3_OPTS)
    df["date_key"] = pd.to_datetime(df["date_key"].astype(str))
    
    # Cast Decimal objects to float
    for col in ["gross_revenue", "net_revenue", "discount_amount", "avg_order_value"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df

@st.cache_data(ttl=3600, show_spinner=False)
def load_customer_daily():
    df = pd.read_parquet(os.path.join(BASE_PATH, "fct_customer_daily_snapshot"), storage_options=S3_OPTS)
    df["date_key"] = pd.to_datetime(df["date_key"].astype(str))
    
    # Cast Decimal objects to float
    for col in ["daily_net_revenue", "cumulative_ltv", "avg_order_gap_days"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df

@st.cache_data(ttl=3600, show_spinner=False)
def load_customer_rfm():
    df = pd.read_parquet(os.path.join(BASE_PATH, "fct_customer_rfm"), storage_options=S3_OPTS)
    df["monetary"] = pd.to_numeric(df["monetary"], errors="coerce").fillna(0.0)
    return df

@st.cache_data(ttl=3600, show_spinner=False)
def load_dim_date():
    df = pd.read_parquet(os.path.join(BASE_PATH, "dim_date"), storage_options=S3_OPTS)
    df["date_key"] = pd.to_datetime(df["date_key"].astype(str))
    return df
