"""
to_gold_customer_daily.py
-------------------------
Gold -> Gold: fct_customer_daily_snapshot

One row per CUSTOMER per DAY within their active range.
Core table for Customer Lifetime Value (PRIMARY GOAL).
Uses window functions for cumulative LTV, churn detection, and CLV segmentation.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_spark_session, get_path, write_parquet_local, read_parquet_local

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DecimalType, IntegerType


def run():
    spark = get_spark_session("to_gold_customer_daily")
    spark.sparkContext.setLogLevel("WARN")
    print("[gold_customer_daily] Starting job ...")

    # ── Read Gold dependencies ───────────────────────────────────────────────
    dim_customer = read_parquet_local(spark, get_path("gold") + "/dim_customer")
    dim_date     = read_parquet_local(spark, get_path("gold") + "/dim_date")
    fct_orders   = read_parquet_local(spark, get_path("gold") + "/fct_order_summary")

    # ── Build the date spine per customer ─────────────────────────────────────
    # Cross-join customers with dates, filtered to each customer's active range
    customers = dim_customer.select(
        "user_id", "is_loyalty", "first_order_date", "last_order_date"
    )

    spine = (
        customers
        .crossJoin(dim_date.select("date_key"))
        .filter(
            (F.col("date_key") >= F.col("first_order_date")) &
            (F.col("date_key") <= F.col("last_order_date"))
        )
    )

    print(f"[gold_customer_daily] Spine rows: {spine.count()}")

    # ── Aggregate daily orders from fct_order_summary ─────────────────────────
    daily_orders = (
        fct_orders
        .groupBy("user_id", "date_key")
        .agg(
            F.count("order_id").alias("daily_order_count"),
            F.sum("gross_revenue").alias("daily_gross_revenue"),
            F.sum("net_revenue").alias("daily_net_revenue"),
            F.sum("discount_amount").alias("daily_discount_amount"),
        )
    )

    # ── Left-join daily activity onto the spine ───────────────────────────────
    result = spine.join(daily_orders, ["user_id", "date_key"], "left")
    result = result.fillna({
        "daily_order_count": 0,
        "daily_gross_revenue": 0,
        "daily_net_revenue": 0,
        "daily_discount_amount": 0,
    })

    # ── Window functions ──────────────────────────────────────────────────────
    user_window = (
        Window.partitionBy("user_id")
        .orderBy("date_key")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # Cumulative metrics
    result = (
        result
        .withColumn("cumulative_order_count",
                    F.sum("daily_order_count").over(user_window).cast(IntegerType()))
        .withColumn("cumulative_ltv",
                    F.sum("daily_net_revenue").over(user_window).cast(DecimalType(10, 2)))
    )

    # Days since last order: find the most recent date with an order
    result = result.withColumn(
        "last_order_date_so_far",
        F.last(
            F.when(F.col("daily_order_count") > 0, F.col("date_key")),
            ignorenulls=True
        ).over(user_window)
    )
    result = result.withColumn(
        "days_since_last_order",
        F.datediff(F.col("date_key"), F.col("last_order_date_so_far"))
    )

    # Average order gap: (last_order_so_far - first_order) / (cumulative_orders - 1)
    result = result.withColumn(
        "avg_order_gap_days",
        F.when(
            F.col("cumulative_order_count") > 1,
            (F.datediff(F.col("last_order_date_so_far"), F.col("first_order_date"))
             / (F.col("cumulative_order_count") - 1)).cast(DecimalType(10, 2))
        ).otherwise(F.lit(None).cast(DecimalType(10, 2)))
    )

    # ── CLV Segmentation (percentile-based, recalculated per date) ────────────
    date_window = Window.partitionBy("date_key").orderBy("cumulative_ltv")
    result = result.withColumn("pct_rank", F.percent_rank().over(date_window))
    result = result.withColumn(
        "clv_segment",
        F.when(F.col("pct_rank") >= 0.8, "High")
         .when(F.col("pct_rank") >= 0.2, "Medium")
         .otherwise("Low")
    )

    # ── Churn Risk Flag ───────────────────────────────────────────────────────
    result = result.withColumn(
        "churn_risk_flag",
        F.when(F.col("days_since_last_order") <= 30, "Active")
         .when(F.col("days_since_last_order") <= 90, "At Risk")
         .otherwise("Churned")
    )

    # ── Clean up helper columns ───────────────────────────────────────────────
    result = result.drop("last_order_date_so_far", "pct_rank",
                         "first_order_date", "last_order_date")

    row_count = result.count()
    print(f"[gold_customer_daily] fct_customer_daily_snapshot: {row_count} rows")

    output_path = get_path("gold") + "/fct_customer_daily_snapshot"
    write_parquet_local(result, output_path)

    print("[gold_customer_daily] Done.")
    spark.stop()


if __name__ == "__main__":
    run()
