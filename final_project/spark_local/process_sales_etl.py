"""
PySpark ETL Job: process_sales (Local)
Pipeline: raw → bronze → silver
Description: Process sales data with data cleansing and partitioning
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, DateType
from datetime import datetime
import os
import re

# Get base directory first to set absolute warehouse path
base_dir = os.environ.get('DATA_LAKE_BASE_DIR', os.path.join(os.path.dirname(__file__), '..'))
base_dir = os.path.abspath(base_dir)
warehouse_dir = os.path.join(base_dir, "spark-warehouse")

spark = (
    SparkSession.builder
    .appName("process_sales_etl_local")
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

# =======================
# STEP 1: RAW → BRONZE
# =======================

print("\n=== Step 1: Reading raw data to bronze ===")

raw_schema = "CustomerId STRING, PurchaseDate STRING, Product STRING, Price STRING"
raw_path = os.path.join(raw_dir, "sales", "*", "*.csv")
print(f"Reading from: {raw_path}")

bronze_df = (
    spark.read
    .option("header", "true")
    .schema(raw_schema)
    .csv(raw_path)
)

bronze_df = bronze_df.withColumn(
    "partition_date",
    F.regexp_extract(F.input_file_name(), r"/(\d{4}[-/]\d{2}[-/]\d{2})/", 1)
)

bronze_path = os.path.join(bronze_dir, "sales")
print(f"Writing bronze: {bronze_path}")

(
    bronze_df.write
    .mode("overwrite")
    .option("path", bronze_path)
    .partitionBy("partition_date")
    .format("parquet")
    .saveAsTable("bronze_sales")
)

print(f"Bronze layer written. Records: {bronze_df.count()}")

# =======================
# STEP 2: BRONZE → SILVER
# =======================

print("\n=== Step 2: Transforming bronze to silver ===")

bronze_df = spark.read.table("bronze_sales")

def parse_date_py(date_str: str):
    if not date_str:
        return None

    # нормалізуємо роздільник
    date_str = date_str.replace("/", "-")

    # yyyy-m-d → yyyy-mm-dd
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", date_str)
    if m:
        y, mo, d = m.groups()
        date_str = f"{y}-{mo.zfill(2)}-{d.zfill(2)}"

    formats = [
        "%Y-%m-%d",    # 2022-09-01 / 2022-9-1 (нормалізується)
        "%m-%d-%Y",    # 09-01-2022
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue

    return None

parse_date = F.udf(parse_date_py, DateType())

silver_df = (
    bronze_df
    .select(
        F.col("CustomerId").cast(IntegerType()).alias("client_id"),
        parse_date(F.col("PurchaseDate")).alias("purchase_date"),
        F.trim(F.lower(F.col("Product"))).alias("product_name"),
        F.regexp_replace(F.col("Price"), "[^0-9.]", "").cast(DecimalType(10, 2)).alias("price"),
        F.col("partition_date")
    )
    .filter(
        F.col("client_id").isNotNull()
        & F.col("purchase_date").isNotNull()
        & F.col("product_name").isNotNull()
        & F.col("price").isNotNull()
    )
)

silver_path = os.path.join(silver_dir, "sales")
print(f"Writing silver: {silver_path}")

(
    silver_df.write
    .mode("overwrite")
    .option("path", silver_path)
    .partitionBy("purchase_date")
    .format("parquet")
    .saveAsTable("silver_sales")
)

print(f"Silver layer written. Records: {silver_df.count()}")

print("\n=== Job completed successfully ===")
spark.stop()
