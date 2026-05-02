import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_spark_session, get_path, write_parquet_local, read_parquet_local

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType, DecimalType

def run():
    spark = get_spark_session("to_silver_order_items")
    spark.sparkContext.setLogLevel("WARN")
    print("[silver_order_items] Starting job ...")

    # 1. Read Bronze
    bronze_path = get_path("bronze") + "/order_items"
    print(f"[silver_order_items] Reading Bronze from: {bronze_path}")
    df = read_parquet_local(spark, bronze_path)
    
    row_count = df.count()

    # Rename columns to lower_snake_case
    for col_name in df.columns:
        if col_name.isupper() or " " in col_name:
            df = df.withColumnRenamed(col_name, col_name.lower())

    # 2. Transformations
    df_silver = (
        df.withColumn("order_timestamp", F.to_timestamp(F.col("creation_time_utc")))
          .withColumn("order_date", F.to_date(F.col("creation_time_utc")))
          .withColumn("user_id", F.coalesce(F.col("user_id"), F.lit("GUEST")))
          .withColumn("item_price", F.col("item_price").cast(DecimalType(10, 2)))
          .withColumn("item_quantity", F.col("item_quantity").cast(IntegerType()))
          .withColumn("line_item_revenue", (F.col("item_price").cast(DecimalType(10, 2)) * F.col("item_quantity").cast(IntegerType())).cast(DecimalType(10, 2)))
          .withColumn("is_loyalty", F.col("is_loyalty").cast(BooleanType()))
          .withColumn(
              "item_category",
              F.when(F.col("item_category").contains("BBQ Plateshttps"), "BBQ Plates")
               .when(F.col("item_category").contains("Drip Chttps"), "Drip Coffee")
               .when(F.col("item_category").contains("Kid'shttps"), "Kids")
               .when(F.col("item_category") == "Kid's", "Kids")
               .when(F.col("item_category") == "Sqalads", "Salads")
               .when(F.col("item_category") == "Bowls0", "Bowls")
               .when(F.col("item_category").startswith("Sandwiches"), "Sandwiches")
               .otherwise(F.col("item_category"))
          )
    )

    # Filter out test data
    df_silver = df_silver.filter(F.col("item_category") != "Test Items")

    # Filter out extreme outliers (likely developer testing)
    df_silver = df_silver.filter(
        (F.col("item_price") < 1000) & 
        (F.col("item_quantity") < 100) & 
        (F.col("line_item_revenue") < 2000)
    )

    # Exclude confirmed developer/QA test accounts.
    # Evidence: These accounts exhibit bot-like ordering patterns that no real
    # customer would produce. Specifically, 26-56% of their orders are placed
    # within 5 minutes of a previous order ("rapid-fire"), they place 20-154
    # orders in a single day, and they hop across 6-19 restaurant locations.
    # By contrast, legitimate high-volume customers in this dataset never
    # exceed 3 orders/day, have <3% rapid-fire rates, and visit 1-3 locations.
    test_users = [
        "5ecf9eda505ee9682b445912",  # 664 orders, 30% rapid, 10 locs
        "5ece77fe902ad501337b23fd",  # 2124 orders, 38% rapid, 19 locs, 154/day peak
        "5f1b00e5535ee93e0cb768e7",  # 1803 orders, 49% rapid, 19 locs
        "5f3c1860505ee9de2a7b23f0",  # 743 orders, 41% rapid, 12 locs
        "60388a4d4f5ee9231e8dda0f",  # 234 orders, 27% rapid, 10 locs
        "5ecf54cf4f5ee92f387b23f3",  # 725 orders, 30% rapid, 14 locs
        "5fcc79d04f5ee9f812be49c2",  # 799 orders, 27% rapid, 12 locs
        "5f184fc74f5ee9e640b4f7ea",  # 380 orders, 30% rapid, 12 locs
        "5ff415c0505ee9d953b281c4",  # 297 orders, 26% rapid, 7 locs
        "5ecf6d334f5ee97259ce6751",  # 118 orders, 32% rapid, 7 locs
        "5eac88d6902ad598127b240b",  # 1000 orders, 56% rapid, 6 locs, 115/day peak
        "5f3d955c37ab46de2a7b240b",  # 525 orders, 28% rapid, 20 locs
        "5f17eb6d505ee9293ccf7dc1",  # 523 orders, 39% rapid, 2 locs
        "5ef4983d505ee9904cedf505",  # 320 orders, 34% rapid, 9 locs
    ]
    df_silver = df_silver.filter(~F.col("user_id").isin(test_users))

    # We partition by order_date_partition (YYYY-MM-DD string) to be consistent with Bronze
    df_silver = df_silver.withColumn("order_date_partition", F.col("order_date").cast("string"))

    # 3. Write Silver
    output_path = get_path("silver") + "/order_items"
    print(f"[silver_order_items] Writing Silver to: {output_path}")
    write_parquet_local(df_silver, output_path, partition_cols=["order_date_partition"])

    print(f"[silver_order_items] Done. {row_count} rows written to Silver.")
    spark.stop()

if __name__ == "__main__":
    run()
