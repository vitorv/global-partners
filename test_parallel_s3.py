import boto3
from botocore.config import Config
from urllib.parse import urlparse
import os
import time
from concurrent.futures import ThreadPoolExecutor

full_path = "s3://global-partners-data-lake-561764228129/gold/fct_daily_sales_summary"
parsed = urlparse(full_path)
bucket = parsed.netloc
prefix = parsed.path.lstrip('/')

config = Config(max_pool_connections=50)
s3 = boto3.client('s3', region_name='us-east-1', config=config)

paginator = s3.get_paginator('list_objects_v2')

local_dir = f"C:/tmp/{prefix.replace('/', '_')}"
os.makedirs(local_dir, exist_ok=True)

keys_to_download = []
for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    if 'Contents' in page:
        for obj in page['Contents']:
            key = obj['Key']
            if key.endswith('.parquet'):
                keys_to_download.append(key)

print(f"Found {len(keys_to_download)} files to download.")

def download_key(key):
    relative_path = key[len(prefix):].lstrip('/')
    local_file = os.path.join(local_dir, relative_path)
    os.makedirs(os.path.dirname(local_file), exist_ok=True)
    if not os.path.exists(local_file):
        s3.download_file(bucket, key, local_file)

start = time.time()
with ThreadPoolExecutor(max_workers=50) as executor:
    list(executor.map(download_key, keys_to_download))

print(f"Time taken to download {len(keys_to_download)} files: {time.time() - start:.2f}s")
