"""
Airflow DAG: process_customers (Local PySpark)
Pipeline: raw → bronze → silver
Schedule: Daily
Description: Process customers data (local execution)
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

def get_var(name, default=None):
    return os.environ.get(name) or Variable.get(name, default_var=default)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

DATA_LAKE_BASE_DIR = get_var("DATA_LAKE_BASE_DIR", default=AIRFLOW_HOME)
SPARK_SUBMIT_BIN = get_var("SPARK_SUBMIT_BIN", default="spark-submit")
SPARK_MASTER = get_var("SPARK_MASTER", default="local[*]")
SPARK_DRIVER_MEMORY = get_var("SPARK_DRIVER_MEMORY", default="2g")
SPARK_EXECUTOR_MEMORY = get_var("SPARK_EXECUTOR_MEMORY", default="2g")

CUSTOMERS_ETL_SCRIPT = get_var(
    "CUSTOMERS_ETL_SCRIPT",
    default=os.path.join(DATA_LAKE_BASE_DIR, "spark_local", "process_customers_etl.py"),
)

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="process_customers_local",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["local", "customers", "spark"],
) as dag:

    process_customers = BashOperator(
        task_id="process_customers_etl",
        bash_command=f"""
        export DATA_LAKE_BASE_DIR="{DATA_LAKE_BASE_DIR}"
        "{SPARK_SUBMIT_BIN}" \
          --master "{SPARK_MASTER}" \
          --driver-memory "{SPARK_DRIVER_MEMORY}" \
          --executor-memory "{SPARK_EXECUTOR_MEMORY}" \
          "{CUSTOMERS_ETL_SCRIPT}"
        """
    )

