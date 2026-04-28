"""
to_bronze_order_items.py
------------------------
Landing → Bronze: order_items table

Source columns (all read as STRING in Bronze):
  APP_NAME, RESTAURANT_ID, CREATION_TIME_UTC, ORDER_ID, USER_ID,
  PRINTED_CARD_NUMBER, IS_LOYALTY, CURRENCY, LINEITEM_ID,
  ITEM_CATEGORY, ITEM_NAME, ITEM_PRICE, ITEM_QUANTITY

PARTITION STRATEGY:
  order_items (~203K rows) partitioned by the DATE portion of
  CREATION_TIME_UTC.  We extract just the date string (YYYY-MM-DD)
  as a new column `order_date_partition` for partition pruning.
  This matches the pattern we'll use in Gold: queries filtered
  by date will only scan the relevant Parquet files.

NOTE ON NULL user_id:
  ~2K rows have a null USER_ID (guest checkouts).  We do NOT fill
  these in Bronze — that's a Silver-layer business rule.  Bronze
  preserves the source data exactly.
"""

import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_spark_session, get_path, ENV, write_parquet_local

from pyspark.sql import functions as F


def run():
    spark = get_spark_session("to_bronze_order_items")
    spark.sparkContext.setLogLevel("WARN")

    print("[bronze_order_items] Starting job ...")
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    # ── 2. Read source ────────────────────────────────────────────────────────
    if ENV == "local":
        source_path = os.path.join(get_path("source_csv"), "order_items.csv")
        print(f"[bronze_order_items] Reading CSV from: {source_path}")
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")      # all STRING in Bronze
            .option("multiLine", "true")
            .csv(source_path)
        )
    else:
        landing_path = get_path("landing") + "order_items/"
        print(f"[bronze_order_items] Reading from S3 landing: {landing_path}")
        df = spark.read.parquet(landing_path)

    # ── 3. Add metadata + partition column ────────────────────────────────────
    df = df.withColumn("ingestion_timestamp", F.lit(ingestion_ts))

    # Extract YYYY-MM-DD from the ISO-8601 timestamp string for partitioning.
    # We do this as a string slice — no type casting — to stay true to Bronze.
    # e.g.  "2023-03-08T11:03:32.223Z"  →  "2023-03-08"
    df = df.withColumn(
        "order_date_partition",
        F.substring(F.col("CREATION_TIME_UTC"), 1, 10)
    )

    row_count = df.count()
    print(f"[bronze_order_items] Rows read from source: {row_count}")

    # ── 4. Write Bronze Parquet ───────────────────────────────────────────────
    output_path = get_path("bronze") + "/order_items"
    print(f"[bronze_order_items] Writing Bronze Parquet to: {output_path}")

    write_parquet_local(df, output_path, partition_cols=["order_date_partition"])

    print(f"[bronze_order_items] Done. {row_count} rows written to Bronze.")
    spark.stop()


if __name__ == "__main__":
    run()
