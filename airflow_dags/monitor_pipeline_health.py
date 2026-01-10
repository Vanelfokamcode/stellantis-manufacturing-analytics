"""
DAG: monitor_pipeline_health
Schedule: Every hour
Purpose: Monitor pipeline health and data quality
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
    dag_id='monitor_pipeline_health',
    default_args=default_args,
    description='Monitor data quality and pipeline health',
    schedule='@hourly',
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['monitoring', 'health'],
) as dag:

    def check_data_quality_score(**context):
        """Calculate overall data quality score"""
        
        conn = psycopg2.connect(
            host='localhost',
            database='stellantis_manufacturing',
            user='vanel',
            password='2003'  # ← Your password
        )
        cursor = conn.cursor()
        
        print("=" * 70)
        print("📊 DATA QUALITY HEALTH CHECK")
        print("=" * 70)
        
        # Check production marts health
        checks = []
        
        # Check 1: Row counts
        try:
            cursor.execute("""
                SELECT 
                    'mart_executive_kpis' as mart,
                    COUNT(*) as row_count
                FROM dbt_prod_marts.mart_executive_kpis
                UNION ALL
                SELECT 
                    'mart_line_performance',
                    COUNT(*)
                FROM dbt_prod_marts.mart_line_performance
                UNION ALL
                SELECT 
                    'mart_maintenance_overview',
                    COUNT(*)
                FROM dbt_prod_marts.mart_maintenance_overview
            """)
            
            print("\nRow Counts:")
            print("-" * 70)
            for row in cursor.fetchall():
                mart, count = row
                status = "✅" if count > 0 else "❌"
                print(f"{status} {mart}: {count} rows")
                checks.append(count > 0)
        except Exception as e:
            print(f"❌ Error checking row counts: {e}")
            checks.append(False)
        
        # Check 2: OEE values are valid
        try:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE oee_percent BETWEEN 0 AND 100) as valid
                FROM dbt_prod_intermediate.int_production_enriched
            """)
            total, valid = cursor.fetchone()
            quality_pct = (valid / total * 100) if total > 0 else 0
            status = "✅" if quality_pct >= 95 else "⚠️"
            print(f"\n{status} OEE Data Quality: {quality_pct:.1f}% valid ({valid}/{total})")
            checks.append(quality_pct >= 95)
        except Exception as e:
            print(f"❌ Error checking OEE: {e}")
            checks.append(False)
        
        # Overall health
        health_score = (sum(checks) / len(checks) * 100) if checks else 0
        
        print("\n" + "=" * 70)
        print(f"OVERALL HEALTH SCORE: {health_score:.1f}%")
        
        if health_score >= 90:
            print("✅ EXCELLENT - All systems healthy")
        elif health_score >= 70:
            print("⚠️  GOOD - Minor issues detected")
        else:
            print("🚨 CRITICAL - Action required!")
        
        print("=" * 70)
        
        cursor.close()
        conn.close()
        
        return health_score
    
    health_check = PythonOperator(
        task_id='data_quality_health',
        python_callable=check_data_quality_score,
    )
