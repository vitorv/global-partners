"""
to_gold_customer_rfm.py
-----------------------
Gold -> Gold: fct_customer_rfm

Single snapshot RFM segmentation as of the last date in the dataset.
One row per customer with Recency, Frequency, Monetary scores (1-5)
and a segment label.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_spark_session, get_path, write_parquet_local, read_parquet_local

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DecimalType, IntegerType


def run():
    spark = get_spark_session("to_gold_customer_rfm")
    spark.sparkContext.setLogLevel("WARN")
    print("[gold_customer_rfm] Starting job ...")

    # ── Read Gold dependency ─────────────────────────────────────────────────
    fct_orders = read_parquet_local(spark, get_path("gold") + "/fct_order_summary")

    # ── Determine the reference date (last order in dataset) ──────────────────
    max_date = fct_orders.agg(F.max("date_key")).collect()[0][0]
    print(f"[gold_customer_rfm] Reference date: {max_date}")

    # ── Compute raw RFM metrics per customer ──────────────────────────────────
    rfm = (
        fct_orders
        .groupBy("user_id")
        .agg(
            F.datediff(F.lit(max_date), F.max("date_key")).alias("recency_days"),
            F.count("order_id").alias("frequency"),
            F.sum("net_revenue").alias("monetary"),
        )
    )

    rfm = rfm.withColumn("date_key", F.lit(max_date))

    # ── Assign 1-5 quintile scores ───────────────────────────────────────────
    # Recency: LOWER is better -> invert the ntile
    # Frequency & Monetary: HIGHER is better -> direct ntile
    rfm = (
        rfm
        .withColumn("r_score",
            (F.lit(6) - F.ntile(5).over(Window.orderBy("recency_days"))).cast(IntegerType()))
        .withColumn("f_score",
            F.ntile(5).over(Window.orderBy("frequency")).cast(IntegerType()))
        .withColumn("m_score",
            F.ntile(5).over(Window.orderBy("monetary")).cast(IntegerType()))
        .withColumn("monetary", F.col("monetary").cast(DecimalType(10, 2)))
    )

    # ── Segment rules (from data_model.md) ────────────────────────────────────
    # VIP:          R >= 4 AND F >= 4 AND M >= 4
    # New Customer: F <= 2 AND R >= 4
    # Churn Risk:   R <= 2 AND F <= 2
    # Regular:      Everything else
    rfm = rfm.withColumn(
        "rfm_segment",
        F.when(
            (F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4),
            "VIP"
        )
        .when(
            (F.col("f_score") <= 2) & (F.col("r_score") >= 4),
            "New Customer"
        )
        .when(
            (F.col("r_score") <= 2) & (F.col("f_score") <= 2),
            "Churn Risk"
        )
        .otherwise("Regular")
    )

    row_count = rfm.count()
    print(f"[gold_customer_rfm] fct_customer_rfm: {row_count} rows")

    output_path = get_path("gold") + "/fct_customer_rfm"
    write_parquet_local(rfm, output_path)

    print("[gold_customer_rfm] Done.")
    spark.stop()


if __name__ == "__main__":
    run()
