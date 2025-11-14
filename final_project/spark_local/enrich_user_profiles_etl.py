"""
PySpark ETL Job: enrich_user_profiles (Local)
Pipeline: silver → gold
Description: Enrich customers data with user_profiles data
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import os

# Get base directory first to set absolute warehouse path
base_dir = os.environ.get('DATA_LAKE_BASE_DIR', os.path.join(os.path.dirname(__file__), '..'))
base_dir = os.path.abspath(base_dir)
warehouse_dir = os.path.join(base_dir, "spark-warehouse")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("enrich_user_profiles_etl_local") \
    .config("spark.sql.warehouse.dir", warehouse_dir) \
    .enableHiveSupport() \
    .getOrCreate()
gold_dir = os.path.join(base_dir, 'spark_local', 'gold')

print(f"Base directory: {base_dir}")
print(f"Gold directory: {gold_dir}")

# ========== SILVER → GOLD ==========
print("\n=== Enriching user profiles from silver to gold ===")

# Read silver tables directly from Parquet files
# (Each Spark session has its own metastore, so we read from files instead)
silver_dir = os.path.join(base_dir, 'spark_local', 'silver')

print("Reading silver_customers from Parquet...")
customers_path = os.path.join(silver_dir, "customers")
customers_df = spark.read.parquet(customers_path)

print("Reading silver_user_profiles from Parquet...")
user_profiles_path = os.path.join(silver_dir, "user_profiles")
user_profiles_df = spark.read.parquet(user_profiles_path)

print(f"Customers records: {customers_df.count()}")
print(f"User profiles records: {user_profiles_df.count()}")

# Join customers with user_profiles on email (case-insensitive, trimmed)
# Enrich customers data with user_profiles data
enriched_df = customers_df.alias("c").join(
    user_profiles_df.alias("up"),
    F.lower(F.trim(F.col("c.email"))) == F.lower(F.trim(F.col("up.email"))),
    "left"
).select(
    # Client ID from customers
    F.col("c.client_id").alias("client_id"),
    
    # First name: use customer data if not empty, otherwise use user_profiles
    F.coalesce(
        F.when(F.trim(F.col("c.first_name")) != "", F.trim(F.col("c.first_name"))),
        F.col("up.first_name")
    ).alias("first_name"),
    
    # Last name: use customer data if not empty, otherwise use user_profiles
    F.coalesce(
        F.when(F.trim(F.col("c.last_name")) != "", F.trim(F.col("c.last_name"))),
        F.col("up.last_name")
    ).alias("last_name"),
    
    # Email from customers
    F.col("c.email").alias("email"),
    
    # Registration date from customers
    F.col("c.registration_date").alias("registration_date"),
    
    # State: use customer data if not empty, otherwise use user_profiles
    F.coalesce(
        F.when(F.trim(F.col("c.state")) != "", F.trim(F.col("c.state"))),
        F.col("up.state")
    ).alias("state"),
    
    # Birth date from user_profiles
    F.col("up.birth_date").alias("birth_date"),
    
    # Phone number from user_profiles
    F.col("up.phone_number").alias("phone_number"),
    
    # Calculate age from birth_date (years between birth_date and current date)
    # Using datediff in days divided by 365.25 for accuracy
    F.when(
        F.col("up.birth_date").isNotNull(),
        (F.datediff(F.current_date(), F.col("up.birth_date")) / 365.25).cast(IntegerType())
    ).alias("age")
)

print(f"Enriched records: {enriched_df.count()}")

# Write to gold layer
gold_path = os.path.join(gold_dir, "user_profiles_enriched")
print(f"Writing to gold: {gold_path}")

enriched_df.write \
    .mode("overwrite") \
    .option("path", gold_path) \
    .format("parquet") \
    .saveAsTable("gold_user_profiles_enriched")

print(f"Gold layer written successfully. Records: {enriched_df.count()}")

# Job completion
print("\n=== Job completed successfully ===")
spark.stop()

