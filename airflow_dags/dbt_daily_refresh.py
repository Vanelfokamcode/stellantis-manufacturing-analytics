"""
DAG: dbt_daily_refresh
Schedule: Daily at 2 AM
Purpose: Run full dbt refresh (all models + tests)
"""

from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Path to dbt project
DBT_PROJECT_DIR = '/home/vanel/stellantis-manufacturing-analytics/dbt_project'
VENV_ACTIVATE = 'source /home/vanel/stellantis-manufacturing-analytics/venv_stellantis/bin/activate'

# Define DAG
with DAG(
    dag_id='dbt_daily_refresh',
    default_args=default_args,
    description='Daily dbt models refresh with testing',
    schedule='@daily',
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['dbt', 'production', 'daily'],
) as dag:

    # Task 1: Run dbt deps
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'{VENV_ACTIVATE} && cd {DBT_PROJECT_DIR} && dbt deps',
    )

    # Task 2: Run staging models
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f'{VENV_ACTIVATE} && cd {DBT_PROJECT_DIR} && dbt run --select staging.*',
    )

    # Task 3: Run intermediate models
    dbt_run_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command=f'{VENV_ACTIVATE} && cd {DBT_PROJECT_DIR} && dbt run --select intermediate.*',
    )

    # Task 4: Run dimensions
    dbt_run_dimensions = BashOperator(
        task_id='dbt_run_dimensions',
        bash_command=f'{VENV_ACTIVATE} && cd {DBT_PROJECT_DIR} && dbt run --select dimensions.*',
    )

    # Task 5: Run marts
    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=f'{VENV_ACTIVATE} && cd {DBT_PROJECT_DIR} && dbt run --select marts.*',
    )

    # Task 6: Run all tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'{VENV_ACTIVATE} && cd {DBT_PROJECT_DIR} && dbt test',
    )

    # Task 7: Success notification
    def notify_success(**context):
        print("✅ dbt refresh completed successfully!")
        return "Success!"
        
    success_task = PythonOperator(
        task_id='success_notification',
        python_callable=notify_success,
    )

    # Define dependencies
    dbt_deps >> dbt_run_staging >> dbt_run_intermediate >> dbt_run_dimensions >> dbt_run_marts >> dbt_test >> success_task
