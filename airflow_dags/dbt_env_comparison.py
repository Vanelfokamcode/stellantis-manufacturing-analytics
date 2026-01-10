"""
DAG: dbt_env_comparison
Schedule: Daily at 4 AM
Purpose: Compare dev vs prod environments
"""

from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2

# Default args
default_args = {
    'owner': 'data_team',
    'retries': 1,
}

# Define DAG
with DAG(
    dag_id='dbt_env_comparison',
    default_args=default_args,
    description='Compare dev vs prod data',
    schedule='0 4 * * *',  # 4 AM daily
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['monitoring', 'comparison'],
) as dag:

    def compare_environments(**context):
        """Compare row counts between dev and prod"""
        
        conn = psycopg2.connect(
            host='localhost',
            database='stellantis_manufacturing',
            user='vanel',
            password='2003'  # ← Your password
        )
        cursor = conn.cursor()
        
        # Tables to compare
        tables = [
            'mart_executive_kpis',
            'mart_line_performance',
            'mart_shift_analysis',
            'mart_maintenance_overview',
            'mart_quality_trends',
            'mart_cost_analysis',
        ]
        
        print("=" * 80)
        print("DEV vs PROD ENVIRONMENT COMPARISON")
        print("=" * 80)
        print(f"{'Table':<30} {'Dev Rows':<15} {'Prod Rows':<15} {'Match?':<10}")
        print("-" * 80)
        
        all_match = True
        
        for table in tables:
            # Get dev count
            cursor.execute(f"SELECT COUNT(*) FROM dbt_dev_marts.{table}")
            dev_count = cursor.fetchone()[0]
            
            # Get prod count
            cursor.execute(f"SELECT COUNT(*) FROM dbt_prod_marts.{table}")
            prod_count = cursor.fetchone()[0]
            
            match = "✅" if dev_count == prod_count else "❌"
            if dev_count != prod_count:
                all_match = False
            
            print(f"{table:<30} {dev_count:<15} {prod_count:<15} {match:<10}")
        
        print("=" * 80)
        
        if all_match:
            print("✅ All tables match! Dev and Prod are in sync.")
        else:
            print("⚠️  Discrepancies found! Review differences.")
        
        print("=" * 80)
        
        cursor.close()
        conn.close()
        
        return "Complete"
    
    compare = PythonOperator(
        task_id='compare_dev_prod',
        python_callable=compare_environments,
    )
