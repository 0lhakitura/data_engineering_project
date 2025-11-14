"""
Helper script: Load Spark Parquet data to PostgreSQL
This script loads silver layer Parquet files into PostgreSQL using Spark JDBC
"""

from pyspark.sql import SparkSession
import os
import sys

# PostgreSQL connection details
PG_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
PG_PORT = os.environ.get('POSTGRES_PORT', '5432')
PG_DATABASE = os.environ.get('POSTGRES_DATABASE', 'data_platform')
PG_USER = os.environ.get('POSTGRES_USER', 'postgres')
PG_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'postgres')

# Base directory
base_dir = os.environ.get('DATA_LAKE_BASE_DIR', os.path.join(os.path.dirname(__file__), '..'))
silver_dir = os.path.join(base_dir, 'spark_local', 'silver')

# Initialize Spark session
spark = SparkSession.builder \
    .appName("load_silver_to_postgres") \
    .config("spark.sql.warehouse.dir", "spark-warehouse") \
    .getOrCreate()

# PostgreSQL JDBC URL
jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
properties = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}

print(f"Loading silver data from {silver_dir} to PostgreSQL")
print(f"JDBC URL: {jdbc_url}")

# Load customers
print("\nLoading silver.customers...")
customers_df = spark.read.parquet(os.path.join(silver_dir, "customers"))
customers_df.write \
    .mode("overwrite") \
    .option("createTableColumnTypes", "client_id INTEGER, first_name VARCHAR(255), last_name VARCHAR(255), email VARCHAR(255), registration_date DATE, state VARCHAR(100)") \
    .jdbc(jdbc_url, "silver.customers", properties=properties)
print(f"Loaded {customers_df.count()} customer records")

# Load sales
print("\nLoading silver.sales...")
sales_df = spark.read.parquet(os.path.join(silver_dir, "sales"))
sales_df.write \
    .mode("overwrite") \
    .option("createTableColumnTypes", "client_id INTEGER, purchase_date DATE, product_name VARCHAR(255), price DECIMAL(10,2)") \
    .jdbc(jdbc_url, "silver.sales", properties=properties)
print(f"Loaded {sales_df.count()} sales records")

# Load user_profiles
print("\nLoading silver.user_profiles...")
user_profiles_df = spark.read.parquet(os.path.join(silver_dir, "user_profiles"))
user_profiles_df.write \
    .mode("overwrite") \
    .option("createTableColumnTypes", "email VARCHAR(255), first_name VARCHAR(255), last_name VARCHAR(255), state VARCHAR(100), birth_date DATE, phone_number VARCHAR(50)") \
    .jdbc(jdbc_url, "silver.user_profiles", properties=properties)
print(f"Loaded {user_profiles_df.count()} user profile records")

print("\n=== Data loading completed successfully ===")
spark.stop()

