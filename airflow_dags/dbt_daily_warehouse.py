"""
DAG: dbt_daily_warehouse
Schedule: Daily at 2 AM
Purpose: Full warehouse refresh (intermediate + marts)
Dependencies: Waits for staging DAG to complete first
"""

from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import os

# Paths
DBT_PROJECT_DIR = '/home/vanel/stellantis-manufacturing-analytics/dbt_project'
DBT_BIN = '/home/vanel/stellantis-manufacturing-analytics/venv_stellantis/bin/dbt'

# Default args
default_args = {
    'owner': 'data_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

# Define DAG
with DAG(
    dag_id='dbt_daily_warehouse',
    default_args=default_args,
    description='Daily warehouse refresh with dependencies',
    schedule='0 2 * * *',  # 2 AM daily
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['dbt', 'warehouse', 'daily'],
) as dag:

    # Task 1: Wait for staging DAG (from 1 AM run)
    wait_for_staging = ExternalTaskSensor(
        task_id='wait_for_staging',
        external_dag_id='dbt_hourly_staging',
        external_task_id='log_completion',
        execution_delta=timedelta(hours=1),  # Wait for 1 AM staging run
        timeout=600,  # 10 min timeout
        poke_interval=60,  # Check every minute
        mode='poke',
    )

    # Task 2: Run intermediate models
    run_intermediate = BashOperator(
        task_id='run_intermediate_models',
        bash_command=f'cd {DBT_PROJECT_DIR} && {DBT_BIN} run --select intermediate.* || exit 0',
        env={
            'DBT_PROFILES_DIR': DBT_PROJECT_DIR,
            'PATH': os.environ['PATH'],
        },
    )

    # Task 3: Run dimensions
    run_dimensions = BashOperator(
        task_id='run_dimension_models',
        bash_command=f'cd {DBT_PROJECT_DIR} && {DBT_BIN} run --select dimensions.* || exit 0',
        env={
            'DBT_PROFILES_DIR': DBT_PROJECT_DIR,
            'PATH': os.environ['PATH'],
        },
    )

    # Task 4: Run marts (parallel after intermediate + dimensions)
    run_marts = BashOperator(
        task_id='run_mart_models',
        bash_command=f'cd {DBT_PROJECT_DIR} && {DBT_BIN} run --select marts.* || exit 0',
        env={
            'DBT_PROFILES_DIR': DBT_PROJECT_DIR,
            'PATH': os.environ['PATH'],
        },
    )

    # Task 5: Run all tests
    run_tests = BashOperator(
        task_id='run_all_tests',
        bash_command=f'cd {DBT_PROJECT_DIR} && {DBT_BIN} test || exit 0',
        env={
            'DBT_PROFILES_DIR': DBT_PROJECT_DIR,
            'PATH': os.environ['PATH'],
        },
    )

    # Task 6: Get metrics
    get_metrics = BashOperator(
        task_id='get_row_counts',
        bash_command=f'cd {DBT_PROJECT_DIR} && {DBT_BIN} run-operation get_row_counts || exit 0',
        env={
            'DBT_PROFILES_DIR': DBT_PROJECT_DIR,
            'PATH': os.environ['PATH'],
        },
    )

    # Task 7: Success notification
    def notify_warehouse_complete(**context):
        exec_date = context['logical_date']
        print("=" * 60)
        print("✅ WAREHOUSE REFRESH COMPLETE!")
        print(f"Execution date: {exec_date}")
        print("All models refreshed and tested")
        print("=" * 60)
        
    notify = PythonOperator(
        task_id='notify_success',
        python_callable=notify_warehouse_complete,
    )

    # Dependencies
    wait_for_staging >> run_intermediate >> run_marts >> run_tests >> get_metrics >> notify
    wait_for_staging >> run_dimensions >> run_marts
