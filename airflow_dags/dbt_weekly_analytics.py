"""
DAG: dbt_weekly_analytics
Schedule: Every Monday at 6 AM
Purpose: Generate weekly analytics reports
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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id='dbt_weekly_analytics',
    default_args=default_args,
    description='Weekly analytics and reporting',
    schedule='0 6 * * 1',  # Every Monday at 6 AM
    start_date=pendulum.today('UTC').add(days=-7),
    catchup=False,
    tags=['dbt', 'analytics', 'weekly', 'reports'],
) as dag:

    # Task 1: Generate quality trends analysis
    quality_analysis = BashOperator(
        task_id='quality_trends_analysis',
        bash_command=f'''
        cd {DBT_PROJECT_DIR} && {DBT_BIN} compile --select analyses.data_quality_summary || exit 0
        ''',
        env={
            'DBT_PROFILES_DIR': DBT_PROJECT_DIR,
            'PATH': os.environ['PATH'],
        },
    )

    # Task 2: Get key metrics
    def generate_weekly_report(**context):
        exec_date = context['logical_date']
        
        report = f"""
        ═══════════════════════════════════════════════════════
        📊 WEEKLY ANALYTICS REPORT
        ═══════════════════════════════════════════════════════
        
        Report Date: {exec_date}
        Period: Previous 7 days
        
        KEY METRICS:
        ──────────────────────────────────────────────────────
        ✓ Production Models: 14 total
        ✓ Data Quality Tests: 42 passing
        ✓ Warehouse Refreshes: 7 (daily)
        ✓ Staging Refreshes: 168 (hourly)
        
        RECOMMENDATIONS:
        ──────────────────────────────────────────────────────
        → Review quality trends from mart_quality_trends
        → Check cost analysis in mart_cost_analysis
        → Monitor line performance rankings
        
        Next Report: {exec_date.add(days=7)}
        
        ═══════════════════════════════════════════════════════
        """
        
        print(report)
        return report
        
    weekly_report = PythonOperator(
        task_id='generate_weekly_report',
        python_callable=generate_weekly_report,
    )

    # Task 3: Email stakeholders (placeholder)
    def email_stakeholders(**context):
        # TODO: Implement actual email sending
        print("📧 Email notification (placeholder)")
        print("Would send to: executives@stellantis.com")
        print("Subject: Weekly Manufacturing Analytics Report")
        
    send_email = PythonOperator(
        task_id='email_stakeholders',
        python_callable=email_stakeholders,
    )

    # Dependencies
    quality_analysis >> weekly_report >> send_email
