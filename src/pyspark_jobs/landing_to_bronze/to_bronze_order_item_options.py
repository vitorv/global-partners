"""
to_bronze_order_item_options.py
--------------------------------
Landing → Bronze: order_item_options table

Source columns (all read as STRING in Bronze):
  ORDER_ID, LINEITEM_ID, OPTION_GROUP_NAME, OPTION_NAME,
  OPTION_PRICE, OPTION_QUANTITY

This is a child table of order_items.  Each order_items row
can have zero or more option rows (e.g. "Add American Cheese +$0",
"Upsize to Large +$0.50").

KEY INSIGHT — Negative OPTION_PRICE:
  Some rows have OPTION_PRICE < 0, which represents a discount
  applied at the option level (e.g. a coupon).  Bronze preserves
  these negative values as-is.  Silver will cast them to DECIMAL
  and create an is_discount flag.

PARTITION STRATEGY:
  This table has no timestamp of its own — it references order_items
  via (ORDER_ID, LINEITEM_ID).  We partition by ORDER_ID prefix
  (first 2 chars of the hex ID) to spread data roughly evenly.
  A simpler alternative: no partitioning — at 193K rows the table
  fits in a single reasonably-sized Parquet file.  We choose the
  simpler approach here for Bronze.
"""

import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_spark_session, get_path, ENV, write_parquet_local

from pyspark.sql import functions as F


def run():
    spark = get_spark_session("to_bronze_order_item_options")
    spark.sparkContext.setLogLevel("WARN")

    print("[bronze_order_item_options] Starting job ...")
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    # ── 2. Read source ────────────────────────────────────────────────────────
    if ENV == "local":
        source_path = os.path.join(get_path("source_csv"), "order_item_options.csv")
        print(f"[bronze_order_item_options] Reading CSV from: {source_path}")
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("multiLine", "true")
            .csv(source_path)
        )
    else:
        landing_path = get_path("landing") + "order_item_options/"
        print(f"[bronze_order_item_options] Reading from S3 landing: {landing_path}")
        df = spark.read.parquet(landing_path)

    # ── 3. Add metadata ───────────────────────────────────────────────────────
    df = df.withColumn("ingestion_timestamp", F.lit(ingestion_ts))

    row_count = df.count()
    print(f"[bronze_order_item_options] Rows read from source: {row_count}")

    # ── 4. Write Bronze Parquet (no partitioning — table is small) ────────────
    output_path = get_path("bronze") + "/order_item_options"
    print(f"[bronze_order_item_options] Writing Bronze Parquet to: {output_path}")

    write_parquet_local(df, output_path)

    print(f"[bronze_order_item_options] Done. {row_count} rows written to Bronze.")
    spark.stop()


if __name__ == "__main__":
    run()
