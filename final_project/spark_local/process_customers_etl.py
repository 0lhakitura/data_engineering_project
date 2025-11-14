"""
PySpark ETL Job: process_customers (Local)
Pipeline: raw → bronze → silver
Description: Process customers data (incremental dump, not partitioned)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DateType
import os
import re

# Get base directory first to set absolute warehouse path
base_dir = os.environ.get('DATA_LAKE_BASE_DIR', os.path.join(os.path.dirname(__file__), '..'))
base_dir = os.path.abspath(base_dir)
warehouse_dir = os.path.join(base_dir, "spark-warehouse")

spark = (
    SparkSession.builder
    .appName("process_customers_etl_local")
    .config("spark.sql.warehouse.dir", warehouse_dir)
    .enableHiveSupport()
    .getOrCreate()
)
raw_dir = os.path.join(base_dir, 'data')
bronze_dir = os.path.join(base_dir, 'spark_local', 'bronze')
silver_dir = os.path.join(base_dir, 'spark_local', 'silver')

print(f"Base directory: {base_dir}")
print(f"Raw directory: {raw_dir}")
print(f"Bronze directory: {bronze_dir}")
print(f"Silver directory: {silver_dir}")

print("\n=== Step 1: Reading raw data to bronze ===")

raw_schema = "Id STRING, FirstName STRING, LastName STRING, Email STRING, RegistrationDate STRING, State STRING"

raw_path = os.path.join(raw_dir, "customers", "*", "*.csv")
print(f"Reading from: {raw_path}")

bronze_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .schema(raw_schema)
    .csv(raw_path)
)

bronze_df = bronze_df.withColumn(
    "file_date",
    F.regexp_extract(F.input_file_name(), r"/(\d{4}-\d{2}-\d{2})/", 1)
)

bronze_path = os.path.join(bronze_dir, "customers")
print(f"Writing to bronze: {bronze_path}")

bronze_df.write \
    .mode("overwrite") \
    .option("path", bronze_path) \
    .format("parquet") \
    .saveAsTable("bronze_customers")

print(f"Bronze layer written successfully. Records: {bronze_df.count()}")

print("\n=== Step 2: Transforming bronze to silver ===")

# Read bronze table (created in the same Spark session)
bronze_df = spark.read.table("bronze_customers")

# Multi-format date parser using UDF to handle single-digit days
def parse_date_udf(date_str):
    """Parse date string with multiple format support"""
    if not date_str:
        return None
    
    from datetime import datetime
    
    # Normalize: convert slashes to dashes
    date_str = date_str.replace("/", "-")
    
    # Normalize single-digit days/months: 2022-8-1 -> 2022-08-01
    match = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", date_str)
    if match:
        year, month, day = match.groups()
        date_str = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    
    # Try parsing with normalized format
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except:
        return None

# Register UDF
parse_date = F.udf(parse_date_udf, DateType())

silver_df = bronze_df.select(
    F.col("Id").cast(IntegerType()).alias("client_id"),
    F.when(F.trim(F.col("FirstName")) == "", None)
     .otherwise(F.trim(F.col("FirstName")))
     .alias("first_name"),
    F.when(F.trim(F.col("LastName")) == "", None)
     .otherwise(F.trim(F.col("LastName")))
     .alias("last_name"),
    F.when(F.trim(F.lower(F.col("Email"))) == "", None)
     .otherwise(F.trim(F.lower(F.col("Email"))))
     .alias("email"),
    parse_date(F.col("RegistrationDate")).alias("registration_date"),
    F.when(F.trim(F.col("State")) == "", None)
     .otherwise(F.trim(F.col("State")))
     .alias("state"),
    F.col("file_date")
).filter(
    F.col("client_id").isNotNull() &
    F.col("email").isNotNull()
)

silver_df = silver_df.dropDuplicates(["client_id"])

silver_path = os.path.join(silver_dir, "customers")
print(f"Writing to silver: {silver_path}")

silver_df.write \
    .mode("overwrite") \
    .option("path", silver_path) \
    .format("parquet") \
    .saveAsTable("silver_customers")

print(f"Silver layer written successfully. Records: {silver_df.count()}")

print("\n=== Job completed successfully ===")
spark.stop()

