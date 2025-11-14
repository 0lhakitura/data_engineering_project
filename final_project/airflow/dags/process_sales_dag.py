from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator


def get_var(name: str, default: str | None = None) -> str:
    env_value = os.environ.get(name)
    if env_value:
        return env_value
    return Variable.get(name, default_var=default)


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", os.path.dirname(os.path.dirname(__file__)))

DATA_LAKE_BASE_DIR = get_var("DATA_LAKE_BASE_DIR", default=AIRFLOW_HOME)
SPARK_SUBMIT_BIN = get_var("SPARK_SUBMIT_BIN", default="spark-submit")
SPARK_MASTER = get_var("SPARK_MASTER", default="local[*]")
SPARK_DRIVER_MEMORY = get_var("SPARK_DRIVER_MEMORY", default="2g")
SPARK_EXECUTOR_MEMORY = get_var("SPARK_EXECUTOR_MEMORY", default="2g")

SALES_ETL_SCRIPT = get_var(
    "SALES_ETL_SCRIPT",
    default=os.path.join(DATA_LAKE_BASE_DIR, "spark_local", "process_sales_etl.py"),
)

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="process_sales_local",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["local", "sales", "spark"],
) as dag:
    process_sales_etl = BashOperator(
        task_id="process_sales_etl",
        bash_command=f"""
        export DATA_LAKE_BASE_DIR="{DATA_LAKE_BASE_DIR}"
        "{SPARK_SUBMIT_BIN}" \
          --master "{SPARK_MASTER}" \
          --driver-memory "{SPARK_DRIVER_MEMORY}" \
          --executor-memory "{SPARK_EXECUTOR_MEMORY}" \
          "{SALES_ETL_SCRIPT}"
        """,
    )
