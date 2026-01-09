#!/usr/bin/env python3
"""
================================================================
STELLANTIS MANUFACTURING ANALYTICS
Script: ETL Pipeline Orchestrator
Author: Vanel
Date: January 2025
Description: Master script to run complete ETL pipeline
Business: One-button data refresh, production-ready automation
================================================================
"""

import psycopg2
import subprocess
import sys
import os
from datetime import datetime
import time

# ================================================================
# CONFIGURATION
# ================================================================

DB_CONFIG = {
    'host': 'localhost',
    'database': 'stellantis_manufacturing',
    'user': 'postgres',
    'password': 'postgres'
}

BASE_DIR = os.path.expanduser("~/stellantis-manufacturing-analytics")
SCRIPT_DIR = os.path.join(BASE_DIR, "scripts/etl")

# ================================================================
# LOGGING
# ================================================================

def log(message, level="INFO"):
    """Print timestamped log message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = {
        "INFO": "ℹ️ ",
        "SUCCESS": "✅",
        "ERROR": "❌",
        "WARNING": "⚠️ "
    }.get(level, "  ")
    
    print(f"[{timestamp}] {prefix} {message}")


def log_section(title):
    """Print section header"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


# ================================================================
# DATABASE UTILITIES
# ================================================================

def get_db_connection():
    """Get PostgreSQL connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        log(f"Database connection failed: {e}", "ERROR")
        return None


def get_row_count(conn, schema, table):
    """Get row count for a table"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Exception as e:
        log(f"Failed to get row count for {schema}.{table}: {e}", "ERROR")
        return None


def truncate_staging(conn):
    """Truncate all staging tables"""
    try:
        cursor = conn.cursor()
        log("Truncating staging tables...")
        
        cursor.execute("TRUNCATE TABLE staging.stg_production_metrics;")
        cursor.execute("TRUNCATE TABLE staging.stg_maintenance;")
        cursor.execute("TRUNCATE TABLE staging.stg_quality;")
        
        conn.commit()
        cursor.close()
        log("Staging tables truncated", "SUCCESS")
        return True
    except Exception as e:
        conn.rollback()
        log(f"Truncate staging failed: {e}", "ERROR")
        return False


def truncate_warehouse(conn):
    """Truncate warehouse fact table"""
    try:
        cursor = conn.cursor()
        log("Truncating warehouse fact table...")
        
        cursor.execute("TRUNCATE TABLE warehouse.fact_production_metrics RESTART IDENTITY CASCADE;")
        
        conn.commit()
        cursor.close()
        log("Warehouse fact table truncated", "SUCCESS")
        return True
    except Exception as e:
        conn.rollback()
        log(f"Truncate warehouse failed: {e}", "ERROR")
        return False


# ================================================================
# ETL STEP RUNNERS
# ================================================================

def run_python_script(script_name, description):
    """Run a Python ETL script"""
    log(f"Running: {description}")
    script_path = os.path.join(SCRIPT_DIR, script_name)
    
    if not os.path.exists(script_path):
        log(f"Script not found: {script_path}", "ERROR")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=300  # 5 min timeout
        )
        
        if result.returncode == 0:
            log(f"{description} completed", "SUCCESS")
            # Print last few lines of output
            output_lines = result.stdout.strip().split('\n')
            for line in output_lines[-3:]:
                if line.strip():
                    print(f"    {line}")
            return True
        else:
            log(f"{description} failed (exit code {result.returncode})", "ERROR")
            print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        log(f"{description} timed out after 5 minutes", "ERROR")
        return False
    except Exception as e:
        log(f"{description} error: {e}", "ERROR")
        return False


def run_sql_validation(conn):
    """Run SQL validation checks"""
    log("Running data quality validation...")
    
    try:
        cursor = conn.cursor()
        
        # Quick validation checks
        checks_passed = 0
        total_checks = 4
        
        # Check 1: No orphan foreign keys
        cursor.execute("""
            SELECT COUNT(*) FROM warehouse.fact_production_metrics f
            LEFT JOIN warehouse.dim_date d ON f.date_key = d.date
            WHERE d.date IS NULL
        """)
        if cursor.fetchone()[0] == 0:
            checks_passed += 1
            log("  ✓ No orphan date keys", "SUCCESS")
        else:
            log("  ✗ Orphan date keys found!", "ERROR")
        
        # Check 2: OEE range
        cursor.execute("""
            SELECT COUNT(*) FROM warehouse.fact_production_metrics
            WHERE oee_percent NOT BETWEEN 0 AND 100
        """)
        if cursor.fetchone()[0] == 0:
            checks_passed += 1
            log("  ✓ OEE values valid (0-100%)", "SUCCESS")
        else:
            log("  ✗ Invalid OEE values found!", "ERROR")
        
        # Check 3: No duplicates
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT date_key, line_key, shift_key, COUNT(*)
                FROM warehouse.fact_production_metrics
                GROUP BY date_key, line_key, shift_key
                HAVING COUNT(*) > 1
            ) dups
        """)
        if cursor.fetchone()[0] == 0:
            checks_passed += 1
            log("  ✓ No duplicate records", "SUCCESS")
        else:
            log("  ✗ Duplicate records found!", "ERROR")
        
        # Check 4: Defects <= Production
        cursor.execute("""
            SELECT COUNT(*) FROM warehouse.fact_production_metrics
            WHERE defects > units_produced
        """)
        if cursor.fetchone()[0] == 0:
            checks_passed += 1
            log("  ✓ Defects ≤ Production", "SUCCESS")
        else:
            log("  ✗ Defects > Production found!", "ERROR")
        
        cursor.close()
        
        quality_score = (checks_passed / total_checks) * 100
        log(f"Data Quality Score: {quality_score:.0f}% ({checks_passed}/{total_checks} checks passed)")
        
        return checks_passed == total_checks
        
    except Exception as e:
        log(f"Validation error: {e}", "ERROR")
        return False


# ================================================================
# MAIN ETL PIPELINE
# ================================================================

def run_etl_pipeline():
    """Execute complete ETL pipeline"""
    
    start_time = time.time()
    
    print("\n" + "=" * 70)
    print("  STELLANTIS MANUFACTURING ANALYTICS")
    print("  ETL PIPELINE ORCHESTRATOR")
    print("=" * 70)
    print(f"  Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")
    
    # Connect to database
    log_section("STEP 0: DATABASE CONNECTION")
    conn = get_db_connection()
    if not conn:
        log("Pipeline aborted: Cannot connect to database", "ERROR")
        return False
    
    log("Connected to stellantis_manufacturing database", "SUCCESS")
    
    # Step 1: Truncate Staging
    log_section("STEP 1: PREPARE STAGING AREA")
    if not truncate_staging(conn):
        log("Pipeline aborted: Staging truncate failed", "ERROR")
        conn.close()
        return False
    
    # Step 2: Load Staging Tables
    log_section("STEP 2: EXTRACT & LOAD STAGING")
    
    staging_scripts = [
        ("load_staging_production.py", "Load Production Metrics"),
        ("load_staging_maintenance.py", "Load Maintenance Data"),
        ("load_staging_quality.py", "Load Quality Data")
    ]
    
    for script, description in staging_scripts:
        if not run_python_script(script, description):
            log(f"Pipeline aborted: {description} failed", "ERROR")
            conn.close()
            return False
    
    # Verify staging loads
    log("\nVerifying staging loads...")
    staging_tables = [
        ("staging", "stg_production_metrics"),
        ("staging", "stg_maintenance"),
        ("staging", "stg_quality")
    ]
    
    staging_success = True
    for schema, table in staging_tables:
        count = get_row_count(conn, schema, table)
        if count and count > 0:
            log(f"  {table}: {count:,} rows", "SUCCESS")
        else:
            log(f"  {table}: No data loaded!", "ERROR")
            staging_success = False
    
    if not staging_success:
        log("Pipeline aborted: Staging validation failed", "ERROR")
        conn.close()
        return False
    
    # Step 3: Truncate Warehouse
    log_section("STEP 3: PREPARE WAREHOUSE")
    if not truncate_warehouse(conn):
        log("Pipeline aborted: Warehouse truncate failed", "ERROR")
        conn.close()
        return False
    
    # Step 4: Transform & Load Fact Table
    log_section("STEP 4: TRANSFORM & LOAD WAREHOUSE")
    if not run_python_script("transform_load_fact.py", "Transform & Load Fact Table"):
        log("Pipeline aborted: Fact table load failed", "ERROR")
        conn.close()
        return False
    
    # Verify fact table
    fact_count = get_row_count(conn, "warehouse", "fact_production_metrics")
    if fact_count and fact_count > 0:
        log(f"Fact table loaded: {fact_count:,} rows", "SUCCESS")
    else:
        log("Fact table is empty!", "ERROR")
        conn.close()
        return False
    
    # Step 5: Data Quality Validation
    log_section("STEP 5: DATA QUALITY VALIDATION")
    validation_passed = run_sql_validation(conn)
    
    if not validation_passed:
        log("Warning: Some validation checks failed", "WARNING")
        log("Review validation results above", "WARNING")
    
    # Close connection
    conn.close()
    
    # Step 6: Success Summary
    log_section("PIPELINE COMPLETE!")
    
    elapsed_time = time.time() - start_time
    
    print("\n📊 EXECUTION SUMMARY")
    print("-" * 70)
    print(f"  Start Time:     {datetime.fromtimestamp(start_time).strftime('%H:%M:%S')}")
    print(f"  End Time:       {datetime.now().strftime('%H:%M:%S')}")
    print(f"  Elapsed Time:   {elapsed_time:.1f} seconds")
    print(f"  Staging Rows:   {sum([get_row_count(get_db_connection(), s, t) or 0 for s, t in staging_tables]):,}")
    print(f"  Fact Rows:      {fact_count:,}")
    print(f"  Quality Score:  {'100%' if validation_passed else '<100%'}")
    print("-" * 70)
    
    if validation_passed:
        print("\n✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("   Data warehouse is ready for analytics queries.\n")
        return True
    else:
        print("\n⚠️  ETL PIPELINE COMPLETED WITH WARNINGS")
        print("   Review validation failures above.\n")
        return False


# ================================================================
# ENTRY POINT
# ================================================================

if __name__ == "__main__":
    success = run_etl_pipeline()
    sys.exit(0 if success else 1)
