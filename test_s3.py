import pandas as pd
import os

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
BASE_PATH = "s3://global-partners-data-lake-561764228129/gold"

print("Attempting to read from S3...")
try:
    df = pd.read_parquet(os.path.join(BASE_PATH, "dim_restaurant"))
    print(f"Successfully read {len(df)} rows!")
    print(df.head())
except Exception as e:
    print(f"Error: {e}")
