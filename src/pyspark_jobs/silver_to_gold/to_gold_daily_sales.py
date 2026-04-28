"""
to_gold_daily_sales.py
----------------------
Silver -> Gold: fct_daily_sales_summary

One row per (date_key, restaurant_id, item_category).
Aggregates sales metrics for the Sales Trends and Location Performance dashboards.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_spark_session, get_path, write_parquet_local, read_parquet_local

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def run():
    spark = get_spark_session("to_gold_daily_sales")
    spark.sparkContext.setLogLevel("WARN")
    print("[gold_daily_sales] Starting job ...")

    # ── Read Silver ──────────────────────────────────────────────────────────
    silver_items   = read_parquet_local(spark, get_path("silver") + "/order_items")
    silver_options = read_parquet_local(spark, get_path("silver") + "/order_item_options")

    # ── Aggregate options per line item ───────────────────────────────────────
    options_per_line = (
        silver_options
        .groupBy("order_id", "lineitem_id")
        .agg(
            F.sum(
                F.when(F.col("option_price") >= 0, F.col("option_revenue")).otherwise(F.lit(0))
            ).alias("pos_option_revenue"),
            F.sum(
                F.when(F.col("option_price") < 0, F.abs(F.col("option_revenue"))).otherwise(F.lit(0))
            ).alias("neg_option_revenue"),
            F.max(F.col("is_discount").cast("int")).alias("line_has_discount"),
        )
    )

    # ── Join items with their option aggregates ───────────────────────────────
    enriched = silver_items.join(options_per_line, ["order_id", "lineitem_id"], "left")
    enriched = enriched.fillna({
        "pos_option_revenue": 0,
        "neg_option_revenue": 0,
        "line_has_discount": 0,
    })

    enriched = enriched.withColumn(
        "line_net_revenue",
        F.col("line_item_revenue") + F.col("pos_option_revenue") - F.col("neg_option_revenue")
    )

    # ── Determine per-order discount status ───────────────────────────────────
    # An order has a discount if ANY of its line items have a discount option
    order_discount = (
        enriched
        .groupBy("order_id")
        .agg(F.max("line_has_discount").alias("order_has_discount"))
    )
    enriched = enriched.join(order_discount, "order_id", "left")

    # ── Aggregate by (date_key, restaurant_id, item_category) ─────────────────
    daily_sales = (
        enriched
        .groupBy(
            F.col("order_date").alias("date_key"),
            "restaurant_id",
            "item_category",
        )
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("item_quantity").alias("total_items_sold"),
            F.sum("line_item_revenue").cast(DecimalType(10, 2)).alias("gross_revenue"),
            F.sum("line_net_revenue").cast(DecimalType(10, 2)).alias("net_revenue"),
            F.sum("neg_option_revenue").cast(DecimalType(10, 2)).alias("discount_amount"),
            F.countDistinct(
                F.when(F.col("order_has_discount") == 1, F.col("order_id"))
            ).alias("discounted_order_count"),
            F.countDistinct(
                F.when(F.col("order_has_discount") == 0, F.col("order_id"))
            ).alias("non_discounted_order_count"),
        )
    )

    daily_sales = daily_sales.withColumn(
        "avg_order_value",
        F.when(F.col("total_orders") > 0,
               (F.col("net_revenue") / F.col("total_orders")).cast(DecimalType(10, 2))
        ).otherwise(F.lit(0).cast(DecimalType(10, 2)))
    )

    row_count = daily_sales.count()
    print(f"[gold_daily_sales] fct_daily_sales_summary: {row_count} rows")

    output_path = get_path("gold") + "/fct_daily_sales_summary"
    write_parquet_local(daily_sales, output_path, partition_cols=["date_key"])

    print("[gold_daily_sales] Done.")
    spark.stop()


if __name__ == "__main__":
    run()
