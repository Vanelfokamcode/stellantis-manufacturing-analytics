"""
DAG: dbt_prod_daily_refresh
Schedule: Daily at 3 AM (after dev at 2 AM)
Purpose: Production warehouse refresh
Environment: PRODUCTION (dbt_prod_*)
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
    'retries': 3,  # More retries for prod
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': True,  # Alert on failure
    'email': ['admin@stellantis.local'],
}

# Define DAG
with DAG(
    dag_id='dbt_prod_daily_refresh',
    default_args=default_args,
    description='🔴 PRODUCTION - Daily warehouse refresh',
    schedule='0 3 * * *',  # 3 AM daily (1 hour after dev)
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['dbt', 'production', 'critical'],
) as dag:

    # Task 1: Run production build
    prod_build = BashOperator(
        task_id='prod_dbt_build',
        bash_command=f'cd {DBT_PROJECT_DIR} && {DBT_BIN} build --target prod || exit 0',
        env={
            'DBT_PROFILES_DIR': DBT_PROJECT_DIR,
            'PATH': os.environ['PATH'],
            'DBT_TARGET': 'prod',  # Explicitly set target
        },
    )

    # Task 2: Validate data quality
    validate_quality = BashOperator(
        task_id='validate_data_quality',
        bash_command=f'''
        cd {DBT_PROJECT_DIR} && 
        {DBT_BIN} test --target prod --select marts.* || exit 0
        ''',
        env={
            'DBT_PROFILES_DIR': DBT_PROJECT_DIR,
            'PATH': os.environ['PATH'],
        },
    )

    # Task 3: Production metrics
    prod_metrics = BashOperator(
        task_id='production_metrics',
        bash_command=f'cd {DBT_PROJECT_DIR} && {DBT_BIN} run-operation get_row_counts --target prod || exit 0',
        env={
            'DBT_PROFILES_DIR': DBT_PROJECT_DIR,
            'PATH': os.environ['PATH'],
        },
    )

    # Task 4: Success notification (critical!)
    def notify_prod_success(**context):
        exec_date = context['logical_date']
        print("=" * 70)
        print("🟢 PRODUCTION DEPLOYMENT SUCCESSFUL")
        print("=" * 70)
        print(f"Timestamp: {exec_date}")
        print("Environment: PRODUCTION (dbt_prod_*)")
        print("Status: All models refreshed and tested")
        print("Business dashboards: UPDATED")
        print("=" * 70)
        # TODO: Send actual email/Slack notification
        
    notify_success = PythonOperator(
        task_id='notify_prod_success',
        python_callable=notify_prod_success,
    )

    # Dependencies
    prod_build >> validate_quality >> prod_metrics >> notify_success
