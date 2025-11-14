"""
PySpark Analytical Query (Local)
Question: In which state were the most televisions purchased by customers aged 20 to 30 in the first decade of September?
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# Initialize Spark session
base_dir = os.environ.get('DATA_LAKE_BASE_DIR', os.path.join(os.path.dirname(__file__), '..'))
base_dir = os.path.abspath(base_dir)
warehouse_dir = os.path.join(base_dir, "spark-warehouse")

# Use Spark with Hive support to access tables from metastore
spark = SparkSession.builder \
    .appName("analytical_query_local") \
    .config("spark.sql.warehouse.dir", warehouse_dir) \
    .enableHiveSupport() \
    .getOrCreate()

print(f"Base directory: {base_dir}")
print(f"Warehouse directory: {warehouse_dir}")

print("\n=== Running Analytical Query ===")
print("Question: In which state were the most televisions purchased")
print("by customers aged 20 to 30 in the first decade of September 2022?")

# Try to use tables from metastore first
print("\n=== Checking for tables in metastore ===")
has_gold = False
has_silver_sales = False
use_metastore_tables = False

try:
    tables = spark.sql("SHOW TABLES").collect()
    table_names = [row.tableName for row in tables]
    print(f"Available tables: {table_names}")
    
    has_gold = "gold_user_profiles_enriched" in table_names
    has_silver_sales = "silver_sales" in table_names
    
    if has_gold and has_silver_sales:
        print("\n✓ Using tables from metastore:")
        print("  - gold_user_profiles_enriched")
        print("  - silver_sales")
        # Tables are available, use them directly in the query
        use_metastore_tables = True
    else:
        print("\n⚠ Some tables not found in metastore:")
        print(f"  - gold_user_profiles_enriched: {'✓' if has_gold else '✗'}")
        print(f"  - silver_sales: {'✓' if has_silver_sales else '✗'}")
        print("  Creating views from Parquet files...")
        use_metastore_tables = False
        
except Exception as e:
    print(f"\n⚠ Error accessing metastore: {e}")
    print("  Creating views from Parquet files...")
    use_metastore_tables = False

# If tables not available, create views from Parquet files
if not use_metastore_tables:
    silver_dir = os.path.join(base_dir, 'spark_local', 'silver')
    gold_dir = os.path.join(base_dir, 'spark_local', 'gold')
    
    if not has_silver_sales:
        sales_path = os.path.join(silver_dir, "sales")
        sales_df = spark.read.parquet(sales_path)
        sales_df.createOrReplaceTempView("silver_sales")
        print(f"  Created view 'silver_sales' from {sales_path}")
    
    if not has_gold:
        gold_path = os.path.join(gold_dir, "user_profiles_enriched")
        gold_df = spark.read.parquet(gold_path)
        gold_df.createOrReplaceTempView("gold_user_profiles_enriched")
        print(f"  Created view 'gold_user_profiles_enriched' from {gold_path}")

# Execute query
result_df = spark.sql("""
    SELECT
        u.state,
        COUNT(*) AS tv_count
    FROM gold_user_profiles_enriched u
    JOIN silver_sales s
      ON u.client_id = s.client_id
    WHERE
        u.age BETWEEN 20 AND 30
        AND s.purchase_date BETWEEN DATE('2022-09-01') AND DATE('2022-09-10')
        AND LOWER(s.product_name) LIKE '%tv%'
    GROUP BY u.state
    ORDER BY tv_count DESC
    LIMIT 1
""")

print("\n=== Result ===")
result_df.show(truncate=False)

# Get the result
result = result_df.collect()
if result:
    state = result[0]['state']
    count = result[0]['tv_count']
    print(f"\n✓ Answer: {state} with {count} television purchases")
else:
    print("\n✗ No results found")

print("\n=== Query completed ===")
spark.stop()
