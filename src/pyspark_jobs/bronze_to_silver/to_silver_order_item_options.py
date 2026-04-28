import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_spark_session, get_path, write_parquet_local, read_parquet_local

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, BooleanType

def run():
    spark = get_spark_session("to_silver_order_item_options")
    spark.sparkContext.setLogLevel("WARN")
    print("[silver_order_item_options] Starting job ...")

    # 1. Read Bronze
    bronze_path = get_path("bronze") + "/order_item_options"
    print(f"[silver_order_item_options] Reading Bronze from: {bronze_path}")
    df = read_parquet_local(spark, bronze_path)
    
    row_count = df.count()

    # Rename columns to lower_snake_case
    for col_name in df.columns:
        if col_name.isupper() or " " in col_name:
            df = df.withColumnRenamed(col_name, col_name.lower())

    # 2. Transformations
    df_silver = (
        df.withColumn("option_price", F.col("option_price").cast(DecimalType(10, 2)))
          .withColumn("option_quantity", F.col("option_quantity").cast(IntegerType()))
          .withColumn("option_revenue", (F.col("option_price").cast(DecimalType(10, 2)) * F.col("option_quantity").cast(IntegerType())).cast(DecimalType(10, 2)))
          .withColumn("is_discount", F.col("option_price").cast(DecimalType(10, 2)) < 0)
    )

    # 3. Write Silver
    output_path = get_path("silver") + "/order_item_options"
    print(f"[silver_order_item_options] Writing Silver to: {output_path}")
    write_parquet_local(df_silver, output_path)

    print(f"[silver_order_item_options] Done. {row_count} rows written to Silver.")
    spark.stop()

if __name__ == "__main__":
    run()
