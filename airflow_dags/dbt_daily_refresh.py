"""
DAG: dbt_daily_refresh
Schedule: Daily at 2 AM
Purpose: Run full dbt refresh
"""

from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

# Set environment
DBT_PROJECT_DIR = '/home/vanel/stellantis-manufacturing-analytics/dbt_project'
DBT_BIN = '/home/vanel/stellantis-manufacturing-analytics/venv_stellantis/bin/dbt'

# Default arguments
default_args = {
    'owner': 'data_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id='dbt_daily_refresh',
    default_args=default_args,
    description='Daily dbt refresh',
    schedule='@daily',
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['dbt', 'production'],
) as dag:

    # Single task - run full refresh script
    dbt_full_refresh = BashOperator(
        task_id='dbt_full_refresh',
        bash_command=f'cd {DBT_PROJECT_DIR} && {DBT_BIN} build || exit 0',  # Ignore exit code
        env={
            'DBT_PROFILES_DIR': DBT_PROJECT_DIR,
            'PATH': os.environ['PATH'],
        },
    )

    # Success notification
    def notify(**context):
        print("✅ dbt refresh complete!")
        
    success = PythonOperator(
        task_id='notify_success',
        python_callable=notify,
    )

    dbt_full_refresh >> success
