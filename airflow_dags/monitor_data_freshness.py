"""
DAG: monitor_data_freshness
Schedule: Every 30 minutes
Purpose: Check if data is fresh and alert if stale
"""

from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import psycopg2

# Default args
default_args = {
    'owner': 'data_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define DAG
with DAG(
    dag_id='monitor_data_freshness',
    default_args=default_args,
    description='Monitor data freshness',
    schedule='*/30 * * * *',  # Every 30 minutes
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['monitoring', 'data-quality'],
) as dag:

    def check_data_freshness(**context):
        """Check when data was last updated"""
        
        conn = psycopg2.connect(
            host='localhost',
            database='stellantis_manufacturing',
            user='vanel',
            password='2003'  # ← Your password
        )
        cursor = conn.cursor()
        
        print("=" * 70)
        print("📊 DATA FRESHNESS CHECK")
        print("=" * 70)
        
        stale_tables = []
        
        # Check production marts
        marts_to_check = [
            ('mart_executive_kpis', 'date'),
            ('mart_line_performance', None),
            ('mart_maintenance_overview', None),
        ]
        
        for mart, date_col in marts_to_check:
            try:
                if date_col:
                    cursor.execute(f"""
                        SELECT 
                            MAX({date_col}) as last_date,
                            NOW()::date - MAX({date_col}) as days_old
                        FROM dbt_prod_marts.{mart}
                    """)
                    result = cursor.fetchone()
                    
                    if result and result[0]:
                        last_date = result[0]
                        days_old = result[1].days if result[1] else 0
                        status = "✅" if days_old < 2 else "⚠️"
                        print(f"{status} {mart}: Last data {last_date} ({days_old} days old)")
                        if days_old > 2:
                            stale_tables.append(mart)
                    else:
                        print(f"❌ {mart}: NO DATA!")
                        stale_tables.append(mart)
                else:
                    cursor.execute(f"SELECT COUNT(*) FROM dbt_prod_marts.{mart}")
                    count = cursor.fetchone()[0]
                    status = "✅" if count > 0 else "❌"
                    print(f"{status} {mart}: {count} rows")
                    if count == 0:
                        stale_tables.append(mart)
                        
            except Exception as e:
                print(f"❌ {mart}: ERROR - {str(e)}")
                stale_tables.append(mart)
        
        print("=" * 70)
        cursor.close()
        conn.close()
        
        context['task_instance'].xcom_push(key='stale_tables', value=stale_tables)
        return 'alert_stale_data' if stale_tables else 'data_is_fresh'
    
    # Tasks
    check_freshness = BranchPythonOperator(
        task_id='check_data_freshness',
        python_callable=check_data_freshness,
    )
    
    data_fresh = EmptyOperator(
        task_id='data_is_fresh',
    )
    
    def send_stale_alert(**context):
        stale_tables = context['task_instance'].xcom_pull(
            task_ids='check_data_freshness',
            key='stale_tables'
        )
        print("🚨" * 35)
        print("ALERT: STALE DATA DETECTED!")
        print("🚨" * 35)
        print(f"Stale tables: {', '.join(stale_tables)}")
        print("=" * 70)
        
    alert_stale = PythonOperator(
        task_id='alert_stale_data',
        python_callable=send_stale_alert,
    )
    
    # Dependencies
    check_freshness >> [data_fresh, alert_stale]
