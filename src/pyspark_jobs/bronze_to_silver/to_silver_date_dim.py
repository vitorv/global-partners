import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_spark_session, get_path, write_parquet_local, read_parquet_local

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType

def run():
    spark = get_spark_session("to_silver_date_dim")
    spark.sparkContext.setLogLevel("WARN")
    print("[silver_date_dim] Starting job ...")

    # 1. Read Bronze
    bronze_path = get_path("bronze") + "/date_dim"
    print(f"[silver_date_dim] Reading Bronze from: {bronze_path}")
    df = read_parquet_local(spark, bronze_path)

    row_count = df.count()

    # 2. Transformations
    df_silver = (
        df.withColumn("date_key", F.to_date(F.col("date_key"), "dd-MM-yyyy"))
          .withColumn("month_number", F.col("month").cast(IntegerType()))
          .withColumn("month_name", F.date_format(F.to_date(F.col("date_key"), "dd-MM-yyyy"), "MMMM"))
          .withColumn("week_number", F.col("week").cast(IntegerType()))
          .withColumn("is_weekend", F.col("is_weekend").cast(BooleanType()))
          .withColumn("is_holiday", F.col("is_holiday").cast(BooleanType()))
          .withColumn("year", F.col("year").cast(IntegerType()))
    )
    
    # Drop the original month and week columns
    df_silver = df_silver.drop("month", "week")

    # 3. Write Silver
    output_path = get_path("silver") + "/date_dim"
    print(f"[silver_date_dim] Writing Silver to: {output_path}")
    write_parquet_local(df_silver, output_path, partition_cols=["year"])

    print(f"[silver_date_dim] Done. {row_count} rows written to Silver.")
    spark.stop()

if __name__ == "__main__":
    run()
