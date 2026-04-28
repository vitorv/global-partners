"""
to_gold_order_summary.py
------------------------
Silver -> Gold: fct_order_summary

One row per ORDER. Aggregates line items and options into a single
order-level record. This is the foundational fact table.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_spark_session, get_path, write_parquet_local, read_parquet_local

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def run():
    spark = get_spark_session("to_gold_order_summary")
    spark.sparkContext.setLogLevel("WARN")
    print("[gold_order_summary] Starting job ...")

    # ── Read Silver ──────────────────────────────────────────────────────────
    silver_items   = read_parquet_local(spark, get_path("silver") + "/order_items")
    silver_options = read_parquet_local(spark, get_path("silver") + "/order_item_options")

    # ── Aggregate order_items by order_id ─────────────────────────────────────
    items_agg = (
        silver_items
        .groupBy("order_id")
        .agg(
            F.first("user_id").alias("user_id"),
            F.first("restaurant_id").alias("restaurant_id"),
            F.first("order_date").alias("date_key"),
            F.first("app_name").alias("app_name"),
            F.first("is_loyalty").alias("is_loyalty"),
            F.sum("item_quantity").alias("total_items"),
            F.sum("line_item_revenue").alias("gross_revenue"),
            F.countDistinct("item_category").alias("distinct_categories"),
        )
    )

    # ── Aggregate options by order_id ─────────────────────────────────────────
    options_agg = (
        silver_options
        .groupBy("order_id")
        .agg(
            F.sum(
                F.when(F.col("option_price") >= 0, F.col("option_revenue")).otherwise(F.lit(0))
            ).alias("option_revenue"),
            F.sum(
                F.when(F.col("option_price") < 0, F.abs(F.col("option_revenue"))).otherwise(F.lit(0))
            ).alias("discount_amount"),
            F.max(F.col("is_discount").cast("int")).alias("has_discount_int"),
        )
    )

    # ── Join and compute net_revenue ──────────────────────────────────────────
    order_summary = items_agg.join(options_agg, "order_id", "left")

    order_summary = order_summary.fillna({
        "option_revenue": 0,
        "discount_amount": 0,
        "has_discount_int": 0,
    })

    order_summary = (
        order_summary
        .withColumn("net_revenue",
            (F.col("gross_revenue") + F.col("option_revenue") - F.col("discount_amount")).cast(DecimalType(10, 2))
        )
        .withColumn("has_discount", F.col("has_discount_int") == 1)
        .withColumn("gross_revenue", F.col("gross_revenue").cast(DecimalType(10, 2)))
        .withColumn("option_revenue", F.col("option_revenue").cast(DecimalType(10, 2)))
        .withColumn("discount_amount", F.col("discount_amount").cast(DecimalType(10, 2)))
        .drop("has_discount_int")
    )

    row_count = order_summary.count()
    print(f"[gold_order_summary] fct_order_summary: {row_count} rows")

    output_path = get_path("gold") + "/fct_order_summary"
    write_parquet_local(order_summary, output_path, partition_cols=["date_key"])

    print("[gold_order_summary] Done.")
    spark.stop()


if __name__ == "__main__":
    run()
