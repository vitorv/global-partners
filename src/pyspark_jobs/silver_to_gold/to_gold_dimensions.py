"""
to_gold_dimensions.py
---------------------
Silver -> Gold: dim_customer, dim_restaurant, dim_date

dim_date is EXPANDED beyond the 365-row 2023 calendar to cover the full
order date range (2020-04-21 to 2024-02-21). Calendar attributes are
derived from actual dates; holidays are mapped by month-day from 2023.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_spark_session, get_path, write_parquet_local, read_parquet_local

from pyspark.sql import functions as F
from pyspark.sql import Window


def run():
    spark = get_spark_session("to_gold_dimensions")
    spark.sparkContext.setLogLevel("WARN")
    print("[gold_dimensions] Starting job ...")

    # ── Read Silver ──────────────────────────────────────────────────────────
    silver_items = read_parquet_local(spark, get_path("silver") + "/order_items")
    silver_date  = read_parquet_local(spark, get_path("silver") + "/date_dim")

    # =====================================================================
    # dim_date  — expanded date spine
    # =====================================================================
    min_date = silver_items.agg(F.min("order_date")).collect()[0][0]
    max_date = silver_items.agg(F.max("order_date")).collect()[0][0]
    print(f"[gold_dimensions] Expanding dim_date from {min_date} to {max_date}")

    date_spine = spark.sql(
        f"SELECT explode(sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day)) as date_key"
    )

    dim_date = (
        date_spine
        .withColumn("day_of_week", F.date_format("date_key", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date_key").isin([1, 7]))
        .withColumn("month_name", F.date_format("date_key", "MMMM"))
        .withColumn("month_number", F.month("date_key"))
        .withColumn("week_number", F.weekofyear("date_key"))
        .withColumn("year", F.year("date_key"))
    )

    # Map holidays from 2023 by month-day match
    holidays_2023 = (
        silver_date
        .filter(F.col("is_holiday") == True)
        .select(
            F.date_format("date_key", "MM-dd").alias("month_day"),
            F.col("holiday_name")
        )
        .distinct()
    )

    dim_date = dim_date.withColumn("month_day", F.date_format("date_key", "MM-dd"))
    dim_date = dim_date.join(holidays_2023, "month_day", "left")
    dim_date = (
        dim_date
        .withColumn("is_holiday", F.col("holiday_name").isNotNull())
        .drop("month_day")
    )

    date_count = dim_date.count()
    print(f"[gold_dimensions] dim_date: {date_count} rows")

    output_path = get_path("gold") + "/dim_date"
    write_parquet_local(dim_date, output_path, partition_cols=["year"])

    # =====================================================================
    # dim_restaurant — deduplicate by picking the most frequent app_name
    # =====================================================================
    rest_app_counts = (
        silver_items
        .groupBy("restaurant_id", "app_name")
        .agg(F.count("order_id").alias("cnt"))
    )
    w_rest = Window.partitionBy("restaurant_id").orderBy(F.desc("cnt"))
    dim_restaurant = (
        rest_app_counts
        .withColumn("rn", F.row_number().over(w_rest))
        .filter(F.col("rn") == 1)
        .select("restaurant_id", "app_name")
    )

    rest_count = dim_restaurant.count()
    print(f"[gold_dimensions] dim_restaurant: {rest_count} rows")

    output_path = get_path("gold") + "/dim_restaurant"
    write_parquet_local(dim_restaurant, output_path)

    # =====================================================================
    # dim_customer
    # =====================================================================
    # primary_restaurant_id and primary_app = mode (most frequent)
    # We use a window to rank restaurants per customer by order count
    cust_rest = (
        silver_items
        .groupBy("user_id", "restaurant_id", "app_name")
        .agg(F.count("order_id").alias("order_count"))
    )

    w = Window.partitionBy("user_id").orderBy(F.desc("order_count"))
    cust_rest_ranked = cust_rest.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)

    dim_customer = (
        silver_items
        .groupBy("user_id")
        .agg(
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.last("is_loyalty").alias("is_loyalty"),
        )
    )

    dim_customer = (
        dim_customer
        .join(
            cust_rest_ranked.select("user_id", "restaurant_id", "app_name"),
            "user_id",
            "left"
        )
        .withColumnRenamed("restaurant_id", "primary_restaurant_id")
        .withColumnRenamed("app_name", "primary_app")
    )

    cust_count = dim_customer.count()
    print(f"[gold_dimensions] dim_customer: {cust_count} rows")

    output_path = get_path("gold") + "/dim_customer"
    write_parquet_local(dim_customer, output_path)

    print("[gold_dimensions] Done.")
    spark.stop()


if __name__ == "__main__":
    run()
