"""
Airflow DAG: enrich_user_profiles (Local PySpark)
Pipeline: silver → gold
Schedule: None (manual trigger)
Description: Enrich user_profiles with additional features (local execution)
"""

from datetime import timedelta
import os
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

START = pendulum.datetime(2024, 1, 1, tz="UTC")

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": START,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.realpath(os.path.join(dag_folder, "..", ".."))
spark_script_path = os.path.join(project_root, "spark_local", "enrich_user_profiles_etl.py")

dag = DAG(
    dag_id="enrich_user_profiles_local",
    default_args=default_args,
    description="Enrich user_profiles data from silver to gold (local PySpark)",
    schedule=None,
    catchup=False,
    tags=["user_profiles", "gold", "enrich", "local"],
)

enrich_user_profiles_job = BashOperator(
    task_id="enrich_user_profiles_etl",
    bash_command=(
        'set -euo pipefail; '
        f'export DATA_LAKE_BASE_DIR="{project_root}"; '
        'spark-submit '
        '--master "local[*]" '
        '--driver-memory 2g '
        '--executor-memory 2g '
        f'"{spark_script_path}"'
    ),
    dag=dag,
)

enrich_user_profiles_job
