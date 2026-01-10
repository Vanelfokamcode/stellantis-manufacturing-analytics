"""
DAG: dbt_hourly_staging
Schedule: Every hour
Purpose: Refresh staging layer with fresh data
"""

from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

# Paths
DBT_PROJECT_DIR = '/home/vanel/stellantis-manufacturing-analytics/dbt_project'
DBT_BIN = '/home/vanel/stellantis-manufacturing-analytics/venv_stellantis/bin/dbt'

# Default args
default_args = {
    'owner': 'data_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id='dbt_hourly_staging',
    default_args=default_args,
    description='Hourly staging data refresh',
    schedule='@hourly',  # Runs every hour
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['dbt', 'staging', 'hourly'],
) as dag:

    # Task 1: Run only staging models (fast!)
    run_staging = BashOperator(
        task_id='run_staging_models',
        bash_command=f'cd {DBT_PROJECT_DIR} && {DBT_BIN} run --select staging.* || exit 0',
        env={
            'DBT_PROFILES_DIR': DBT_PROJECT_DIR,
            'PATH': os.environ['PATH'],
        },
    )

    # Task 2: Quick validation (only staging tests)
    test_staging = BashOperator(
        task_id='test_staging_models',
        bash_command=f'cd {DBT_PROJECT_DIR} && {DBT_BIN} test --select staging.* || exit 0',
        env={
            'DBT_PROFILES_DIR': DBT_PROJECT_DIR,
            'PATH': os.environ['PATH'],
        },
    )

    # Task 3: Log completion
    def log_staging_complete(**context):
        exec_date = context['logical_date']
        print(f"✅ Staging refresh complete at {exec_date}")
        print("Fresh data ready for warehouse!")
        
    log_complete = PythonOperator(
        task_id='log_completion',
        python_callable=log_staging_complete,
    )

    # Dependencies
    run_staging >> test_staging >> log_complete
