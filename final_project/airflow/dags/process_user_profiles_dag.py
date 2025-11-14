"""
Airflow DAG: process_user_profiles (Local PySpark)
Pipeline: raw → silver
Schedule: None (manual trigger only)
Description: Process user_profiles JSONL data - local execution
"""

from datetime import timedelta
import os
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
spark_script_path = os.path.join(project_root, "spark_local", "process_user_profiles_etl.py")

dag = DAG(
    dag_id="process_user_profiles_local",
    default_args=default_args,
    description="Process user_profiles data from raw to silver (manual trigger, local PySpark)",
    schedule=None,   # manual only
    catchup=False,
    tags=["user_profiles", "silver", "manual", "local"],
)

process_user_profiles_job = BashOperator(
    task_id="process_user_profiles_etl",
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

trigger_enrichment = TriggerDagRunOperator(
    task_id="trigger_enrich_user_profiles",
    trigger_dag_id="enrich_user_profiles_local",
    wait_for_completion=False,
    dag=dag,
)

process_user_profiles_job >> trigger_enrichment
