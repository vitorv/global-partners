import pandas as pd
import time

start = time.time()
local_dir = 'c:/Workspace/GitRepos/global_partners_project/data/output/gold/fct_customer_daily_snapshot'

print("Reading dates...")
df_dates = pd.read_parquet(local_dir, columns=["date_key"])
latest_date = df_dates["date_key"].max()
print(f"Latest date: {latest_date}")

print("Reading filtered data...")
df = pd.read_parquet(local_dir, filters=[("date_key", "==", latest_date)])
print(df.info(memory_usage='deep'))
print(f"Time: {time.time() - start:.2f}s")
