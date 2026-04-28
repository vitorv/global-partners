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
    )

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
