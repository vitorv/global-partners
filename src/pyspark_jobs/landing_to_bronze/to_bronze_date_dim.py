"""
to_bronze_date_dim.py
---------------------
Landing → Bronze: date_dim table

WHAT THIS SCRIPT DOES:
  1. Reads the raw date_dim data (CSV locally, S3 landing zone in AWS)
  2. Adds a single metadata column: ingestion_timestamp
  3. Writes the result to the Bronze layer as Parquet, partitioned by year

WHY BRONZE?
  Bronze is a verbatim copy of the source — no business logic, no filtering.
  Its sole purpose is to preserve raw data so that if something goes wrong in
  Silver or Gold we can reprocess from here without hitting the source again.

WHY PARQUET?
  Parquet is a columnar binary format.  Compared to CSV it is:
    - ~3–5x smaller on disk (run-length + dictionary compression)
    - Much faster to read for analytical queries (column pruning, predicate pushdown)
    - Self-describing (schema is embedded in the file)

PARTITION STRATEGY:
  date_dim is tiny (365 rows) so partitioning is not strictly necessary, but
  we partition by `year` to be consistent with the rest of the pipeline.
"""

import sys
import os
from datetime import datetime, timezone

# Allow importing config from the parent pyspark_jobs package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_spark_session, get_path, ENV, write_parquet_local

from pyspark.sql import functions as F


def run():
    # ── 1. Initialise Spark ───────────────────────────────────────────────────
    spark = get_spark_session("to_bronze_date_dim")
    spark.sparkContext.setLogLevel("WARN")  # suppress verbose INFO logs

    print("[bronze_date_dim] Starting job ...")
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    # ── 2. Read source data ───────────────────────────────────────────────────
    if ENV == "local":
        # Local dev: read from the CSV in docs/source_data/
        source_path = os.path.join(get_path("source_csv"), "date_dim.csv")
        print(f"[bronze_date_dim] Reading CSV from: {source_path}")
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")   # Bronze always reads everything as STRING
            .option("multiLine", "true")
            .csv(source_path)
        )
    else:
        # AWS: read from the S3 landing zone (already in Parquet or CSV there)
        landing_path = get_path("landing") + "date_dim/"
        print(f"[bronze_date_dim] Reading from S3 landing: {landing_path}")
        df = spark.read.parquet(landing_path)

    # ── 3. Add metadata column ────────────────────────────────────────────────
    # ingestion_timestamp tells downstream jobs (and auditors) exactly when
    # this data was pulled from the source.  It is a STRING in Bronze because
    # Bronze never alters types — that's Silver's job.
    df = df.withColumn("ingestion_timestamp", F.lit(ingestion_ts))

    row_count = df.count()
    print(f"[bronze_date_dim] Rows read from source: {row_count}")

    # ── 4. Write Bronze Parquet ───────────────────────────────────────────────
    output_path = get_path("bronze") + "/date_dim"
    print(f"[bronze_date_dim] Writing Bronze Parquet to: {output_path}")

    write_parquet_local(df, output_path, partition_cols=["year"])

    print(f"[bronze_date_dim] Done. {row_count} rows written to Bronze.")
    spark.stop()


if __name__ == "__main__":
    run()
