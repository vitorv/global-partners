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

def _safe_read_parquet(path_suffix, **kwargs):
    """
    Downloads parquet files from S3 to a local temp directory and reads them.
    This avoids the massive OOM memory leak in s3fs.
    """
    full_path = os.path.join(BASE_PATH, path_suffix).replace('\\', '/')
    
    if not full_path.startswith("s3://"):
        return pd.read_parquet(full_path, **kwargs)
    
    import boto3
    from botocore.config import Config
    from urllib.parse import urlparse
    import tempfile
    from concurrent.futures import ThreadPoolExecutor


    
    parsed = urlparse(full_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip('/')
    
    config = Config(max_pool_connections=50)
    s3 = boto3.client('s3', region_name='us-east-1', config=config)
    paginator = s3.get_paginator('list_objects_v2')
    
    local_dir = f"/tmp/{prefix.replace('/', '_')}"
    os.makedirs(local_dir, exist_ok=True)
    
    keys_to_download = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.parquet'):
                    keys_to_download.append(key)
                    
    if not keys_to_download:
        raise FileNotFoundError(f"No parquet files found in {full_path}")
        
    def download_s3_file(key):
        relative_path = key[len(prefix):].lstrip('/')
        local_file = os.path.join(local_dir, relative_path)
        os.makedirs(os.path.dirname(local_file), exist_ok=True)
        if not os.path.exists(local_file):
            s3.download_file(bucket, key, local_file)

    with ThreadPoolExecutor(max_workers=50) as executor:
        list(executor.map(download_s3_file, keys_to_download))
        
    return pd.read_parquet(local_dir, **kwargs)

@st.cache_resource(ttl=3600, show_spinner=False)
def load_dim_restaurant():
    df = _safe_read_parquet("dim_restaurant")
    # Create a clean label "Alltown Fresh (#abc123)"
    df["location_label"] = df["app_name"] + " (#" + df["restaurant_id"].str[-6:] + ")"
    return df

@st.cache_resource(ttl=3600, show_spinner=False)
def load_dim_customer():
    df = _safe_read_parquet("dim_customer")
    df["first_order_date"] = pd.to_datetime(df["first_order_date"])
    df["last_order_date"] = pd.to_datetime(df["last_order_date"])
    return df

@st.cache_resource(ttl=3600, show_spinner=False)
def load_order_summary():
    df = _safe_read_parquet("fct_order_summary")
    df["date_key"] = pd.to_datetime(df["date_key"].astype(str))
    
    # Cast Decimal objects to float
    for col in ["gross_revenue", "option_revenue", "discount_amount", "net_revenue"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df

@st.cache_resource(ttl=3600, show_spinner=False)
def load_daily_sales():
    df = _safe_read_parquet("fct_daily_sales_summary")
    df["date_key"] = pd.to_datetime(df["date_key"].astype(str))
    
    # Cast Decimal objects to float
    for col in ["gross_revenue", "net_revenue", "discount_amount", "avg_order_value"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df

@st.cache_resource(ttl=3600, show_spinner=False)
def load_customer_daily():
    # Only load dates first to find the max, avoiding a massive 8GB memory spike
    df_dates = _safe_read_parquet("fct_customer_daily_snapshot", columns=["date_key"])
    latest_date = df_dates["date_key"].max()
    
    # Read only the latest snapshot day using PyArrow push-down filters
    df = _safe_read_parquet("fct_customer_daily_snapshot", filters=[("date_key", "==", latest_date)])
    df["date_key"] = pd.to_datetime(df["date_key"].astype(str))
    
    # Cast Decimal objects to float
    for col in ["daily_net_revenue", "cumulative_ltv", "avg_order_gap_days"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df

@st.cache_resource(ttl=3600, show_spinner=False)
def load_customer_rfm():
    df = _safe_read_parquet("fct_customer_rfm")
    df["monetary"] = pd.to_numeric(df["monetary"], errors="coerce").fillna(0.0)
    return df

@st.cache_resource(ttl=3600, show_spinner=False)
def load_dim_date():
    df = _safe_read_parquet("dim_date")
    df["date_key"] = pd.to_datetime(df["date_key"].astype(str))
    return df
