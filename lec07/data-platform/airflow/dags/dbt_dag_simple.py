# airflow/dags/dbt_dag_simple.py
# Спрощена версія без Cosmos - використовує BashOperator
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
from datetime import datetime
import os

# Get environment variables
ANALYTICS_DB = os.getenv('ANALYTICS_DB', 'analytics')
PROJECT_DIR = os.getenv('AIRFLOW_HOME', '/opt/airflow') + "/dags/dbt/my_dbt_project"
PROFILE = 'my_dbt_project'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'custom_dbt_transformations',
    default_args=default_args,
    description='Run dbt transformations with BashOperator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 22),
    catchup=False,
    tags=['dbt', 'custom'],
)

# Profiles directory для dbt (використовуємо директорію де знаходиться profiles.yml)
PROFILES_DIR = os.path.dirname(PROJECT_DIR)

# Step 1: Run dbt run to execute models
# Вказуємо --profiles-dir щоб dbt знайшов profiles.yml
# Use full path to dbt since /home/airflow/.local/bin is not in PATH for BashOperator
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=f'export PATH="/home/airflow/.local/bin:$PATH" && cd {PROJECT_DIR} && dbt run --profiles-dir {PROFILES_DIR} --select example --vars \'{{"is_test": false, "data_date": "{{{{ ds }}}}"}}\'',
    env={
        'ANALYTICS_DB': os.getenv('ANALYTICS_DB', ANALYTICS_DB),
        'POSTGRES_ANALYTICS_HOST': os.getenv('POSTGRES_ANALYTICS_HOST', 'postgres_analytics'),
        'ETL_USER': os.getenv('ETL_USER'),
        'ETL_PASSWORD': os.getenv('ETL_PASSWORD'),
        'DBT_PROFILE': PROFILE,
        'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
    },
    dag=dag,
)

# Step 2: Run dbt test to validate results
# Use full path to dbt since /home/airflow/.local/bin is not in PATH for BashOperator
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=f'export PATH="/home/airflow/.local/bin:$PATH" && cd {PROJECT_DIR} && dbt test --profiles-dir {PROFILES_DIR} --vars \'{{"is_test": false, "data_date": "{{{{ ds }}}}"}}\'',
    env={
        'ANALYTICS_DB': os.getenv('ANALYTICS_DB', ANALYTICS_DB),
        'POSTGRES_ANALYTICS_HOST': os.getenv('POSTGRES_ANALYTICS_HOST', 'postgres_analytics'),
        'ETL_USER': os.getenv('ETL_USER'),
        'ETL_PASSWORD': os.getenv('ETL_PASSWORD'),
        'DBT_PROFILE': PROFILE,
        'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
    },
    dag=dag,
)

# Define the task dependencies
dbt_run >> dbt_test

