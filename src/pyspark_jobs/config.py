"""
config.py
---------
Single source of truth for all connection parameters and data paths.

LOCAL MODE:  reads from RDS via JDBC, writes Parquet to data/output/
AWS MODE:    reads from S3 landing zone, writes Parquet to S3 layers
             (swap the path constants below — no other file needs to change)
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = '--driver-java-options "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" pyspark-shell'

# ---------------------------------------------------------------------------
# ENVIRONMENT DETECTION
# Set PIPELINE_ENV=aws in the Glue job parameters to switch to AWS paths.
# ---------------------------------------------------------------------------
ENV = os.environ.get("PIPELINE_ENV", "local")  # "local" | "aws"

# ---------------------------------------------------------------------------
# RDS / SQL SERVER CONNECTION  (Landing → Bronze only)
# Values are read from environment variables so credentials are never
# committed to source control. Set these in your shell or .env file.
# ---------------------------------------------------------------------------
RDS_HOST     = os.environ.get("RDS_HOST", "")
RDS_PORT     = os.environ.get("RDS_PORT", "1433")
RDS_DATABASE = os.environ.get("RDS_DATABASE", "global_partners")
RDS_USER     = os.environ.get("RDS_USER", "")
RDS_PASSWORD = os.environ.get("RDS_PASSWORD", "")

# JDBC URL — standard format for SQL Server
JDBC_URL = (
    f"jdbc:sqlserver://{RDS_HOST}:{RDS_PORT};"
    f"databaseName={RDS_DATABASE};"
    f"encrypt=true;trustServerCertificate=true"
)

JDBC_PROPERTIES = {
    "user":     RDS_USER,
    "password": RDS_PASSWORD,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# Path to the Microsoft JDBC JAR for SQL Server.
# Download from: https://learn.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server
# Place it at the project root.  Glue has it natively — this is local only.
JDBC_JAR_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "mssql-jdbc-12.6.1.jre11.jar",
)

# ---------------------------------------------------------------------------
# DATA PATHS
# ---------------------------------------------------------------------------

# ── LOCAL (development) ──────────────────────────────────────────────────────
_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

LOCAL_PATHS = {
    # Raw CSVs  (used as a local fallback when RDS is not available)
    "source_csv": os.path.join(_PROJECT_ROOT, "docs", "source_data"),

    # Medallion output layers
    "bronze": os.path.join(_PROJECT_ROOT, "data", "output", "bronze"),
    "silver": os.path.join(_PROJECT_ROOT, "data", "output", "silver"),
    "gold":   os.path.join(_PROJECT_ROOT, "data", "output", "gold"),
}

# ── AWS (production) ─────────────────────────────────────────────────────────
S3_BUCKET = os.environ.get("S3_BUCKET", "global-partners-data-lake")

AWS_PATHS = {
    "landing": f"s3://{S3_BUCKET}/landing/",
    "bronze":  f"s3://{S3_BUCKET}/bronze/",
    "silver":  f"s3://{S3_BUCKET}/silver/",
    "gold":    f"s3://{S3_BUCKET}/gold/",
}

# ---------------------------------------------------------------------------
# ACTIVE PATH RESOLVER
# Call get_path("bronze") anywhere in your scripts — it automatically
# returns the correct local or S3 path based on PIPELINE_ENV.
# ---------------------------------------------------------------------------
def get_path(layer: str) -> str:
    """Return the correct base path for a given medallion layer."""
    if ENV == "aws":
        return AWS_PATHS[layer]
    return LOCAL_PATHS[layer]


# ---------------------------------------------------------------------------
# SPARK SESSION FACTORY
# Centralised so every script gets the same configuration.
# The JDBC JAR is only needed locally; Glue provides it natively.
# ---------------------------------------------------------------------------
def get_spark_session(app_name: str):
    """Create and return a configured SparkSession."""
    import platform
    from pyspark.sql import SparkSession

    # ── Windows setup: must happen BEFORE the JVM starts ─────────────────────
    if platform.system() == "Windows":
        # HADOOP_HOME tells the Hadoop client where winutils.exe lives.
        # Even if winutils is mismatched, setting this silences startup errors.
        os.environ.setdefault("HADOOP_HOME", "C:\\hadoop")
        os.environ.setdefault("hadoop.home.dir", "C:\\hadoop")

    builder = SparkSession.builder.appName(app_name)

    if ENV == "local":
        builder = (
            builder
            .master("local[*]")                         # use all local CPU cores
            .config("spark.driver.memory", "4g")        # enough for 200K-row tables
        )

        if platform.system() == "Windows":
            # Disable Hadoop native I/O on Windows.
            # The NativeIO$Windows.access0 call requires a perfectly-matched
            # hadoop.dll which is difficult to guarantee locally.
            # Pure-Java I/O is correct and fast enough for dev-scale data.
            builder = (
                builder
                .config("spark.hadoop.io.native.lib.available", "false")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            )

        # Only add the JDBC JAR if it has been downloaded (not needed for CSV-mode dev)
        if os.path.exists(JDBC_JAR_PATH):
            builder = builder.config("spark.jars", JDBC_JAR_PATH)

    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# LOCAL PARQUET I/O HELPERS  (Windows-safe)
# ---------------------------------------------------------------------------
# PySpark's native .write.parquet() fails on Windows without a correctly
# compiled winutils.exe / hadoop.dll matched to the exact Hadoop version.
# These helpers detect Windows and use pandas + pyarrow for local I/O instead.
# When PIPELINE_ENV=aws (Glue), we always use native Spark I/O.
#
# From the script author's perspective nothing changes — just call:
#   write_parquet_local(df, path, partition_cols=[...])
#   df = read_parquet_local(spark, path)
# ---------------------------------------------------------------------------

import platform as _platform

_IS_WINDOWS = _platform.system() == "Windows"


def write_parquet_local(df, output_path: str, partition_cols: list = None):
    """
    Write a Spark DataFrame to Parquet at output_path.

    - LOCAL on Windows : converts to pandas → writes via pyarrow (no winutils needed)
    - LOCAL on Linux   : native spark .write.parquet()
    - AWS              : native spark .write.parquet()

    partition_cols : list of column names to partition by (optional)
    """
    import shutil

    if ENV == "aws":
        # Native Spark write to S3
        w = df.write.mode("overwrite")
        if partition_cols:
            w = w.partitionBy(*partition_cols)
        w.parquet(output_path)
        return

    if _IS_WINDOWS:
        # ── pandas/pyarrow path (Windows) ─────────────────────────────────
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Clean the output directory so we get a fresh overwrite
        if os.path.exists(output_path):
            shutil.rmtree(output_path)
        os.makedirs(output_path, exist_ok=True)

        # Convert Spark → pandas
        pdf = df.toPandas()
        table = pa.Table.from_pandas(pdf, preserve_index=False)

        if partition_cols:
            pq.write_to_dataset(
                table,
                root_path=output_path,
                partition_cols=partition_cols,
                max_partitions=2000
            )
        else:
            pq.write_table(table, os.path.join(output_path, "part-0.parquet"))
    else:
        # ── native Spark path (Linux / Mac) ───────────────────────────────
        w = df.write.mode("overwrite")
        if partition_cols:
            w = w.partitionBy(*partition_cols)
        w.parquet(output_path)


def read_parquet_local(spark, input_path: str):
    """
    Read Parquet files from input_path into a Spark DataFrame.
    On Windows (local), uses pyarrow to clean incompatible types
    (dictionary, large_string) then writes a temp Parquet that
    PySpark can read natively via file:// URIs.
    """
    if ENV == "local" and _IS_WINDOWS:
        import pyarrow as pa
        import pyarrow.parquet as pq
        import tempfile

        # Read via pyarrow (handles partitioned datasets natively)
        table = pq.read_table(input_path)

        # Cast dictionary columns to their value types and large_string to string
        # Also normalize decimal precision for PySpark compatibility
        new_fields = []
        for i, field in enumerate(table.schema):
            col = table.column(i)
            if pa.types.is_dictionary(field.type):
                col = col.cast(field.type.value_type)
                field = field.with_type(field.type.value_type)
            if pa.types.is_large_string(field.type):
                col = col.cast(pa.string())
                field = field.with_type(pa.string())
            if pa.types.is_decimal(field.type):
                # PySpark needs precision >= 10 for DECIMAL types written by PyArrow
                new_type = pa.decimal128(max(field.type.precision, 10), field.type.scale)
                col = col.cast(new_type)
                field = field.with_type(new_type)
            if pa.types.is_timestamp(field.type):
                # PySpark can't read TIMESTAMP(NANOS) — downcast to microseconds
                new_type = pa.timestamp("us", tz=field.type.tz)
                col = col.cast(new_type)
                field = field.with_type(new_type)
            new_fields.append((field, col))

        new_schema = pa.schema([f for f, _ in new_fields])
        new_columns = [c for _, c in new_fields]
        table = pa.table(
            {new_schema.field(i).name: new_columns[i] for i in range(len(new_columns))},
            schema=new_schema,
        )

        # Remove pandas metadata that can cause PySpark issues
        table = table.replace_schema_metadata({})

        # Write to a temp Parquet file and read via Spark natively
        tmp_dir = tempfile.mkdtemp(prefix="spark_read_")
        tmp_path = os.path.join(tmp_dir, "data.parquet")
        pq.write_table(table, tmp_path)

        file_uri = "file:///" + tmp_path.replace("\\", "/")
        return spark.read.parquet(file_uri)
    else:
        return spark.read.parquet(input_path)


