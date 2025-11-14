"""
PySpark ETL Job: process_user_profiles (Local)
Pipeline: raw → silver
Description: Process user_profiles JSONL data (high quality, direct to silver)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType
import os

# Get base directory first to set absolute warehouse path
base_dir = os.environ.get('DATA_LAKE_BASE_DIR', os.path.join(os.path.dirname(__file__), '..'))
base_dir = os.path.abspath(base_dir)
warehouse_dir = os.path.join(base_dir, "spark-warehouse")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("process_user_profiles_etl_local") \
    .config("spark.sql.warehouse.dir", warehouse_dir) \
    .enableHiveSupport() \
    .getOrCreate()
raw_dir = os.path.join(base_dir, 'data')
silver_dir = os.path.join(base_dir, 'spark_local', 'silver')

print(f"Base directory: {base_dir}")
print(f"Raw directory: {raw_dir}")
print(f"Silver directory: {silver_dir}")

# ========== RAW → SILVER (direct, no bronze layer) ==========
print("\n=== Processing JSONL data directly to silver ===")

# Read JSONL files from raw/user_profiles/
raw_path = os.path.join(raw_dir, "user_profiles", "*.json")
print(f"Reading from: {raw_path}")

user_profiles_df = spark.read.json(raw_path)

print(f"Raw records read: {user_profiles_df.count()}")

# Transform to silver schema
# Since data quality is perfect, we mainly need to standardize field names
silver_df = user_profiles_df.select(
    # Email as primary key
    F.lower(F.trim(F.col("email"))).alias("email"),
    
    # Parse full_name into first_name and last_name
    F.split(F.col("full_name"), " ")[0].alias("first_name"),
    F.element_at(F.split(F.col("full_name"), " "), -1).alias("last_name"),
    
    # State
    F.trim(F.col("state")).alias("state"),
    
    # Birth date: convert to DATE
    F.to_date(F.col("birth_date"), "yyyy-MM-dd").alias("birth_date"),
    
    # Phone number
    F.col("phone_number").alias("phone_number")
).filter(
    # Filter out records without email
    F.col("email").isNotNull()
)

# Write to silver layer (not partitioned)
silver_path = os.path.join(silver_dir, "user_profiles")
print(f"Writing to silver: {silver_path}")

silver_df.write \
    .mode("overwrite") \
    .option("path", silver_path) \
    .format("parquet") \
    .saveAsTable("silver_user_profiles")

print(f"Silver layer written successfully. Records: {silver_df.count()}")

# Job completion
print("\n=== Job completed successfully ===")
spark.stop()

