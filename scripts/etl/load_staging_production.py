#!/usr/bin/env python3
"""
================================================================
STELLANTIS MANUFACTURING ANALYTICS
Script: Load Staging - Production Metrics
Author: Vanel
Date: January 2025
Description: Extract production_metrics.csv → staging.stg_production_metrics
Business: Preserve raw production data in database
================================================================
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import os

# ================================================================
# CONFIGURATION
# ================================================================

BASE_DIR = os.path.expanduser("~/stellantis-manufacturing-analytics")
CSV_FILE = os.path.join(BASE_DIR, "data/raw/production/production_metrics.csv")

DB_CONFIG = {
    'host': 'localhost',
    'database': 'stellantis_manufacturing',
    'user': 'postgres',
    'password': 'postgres'
}

# ================================================================
# MAIN LOAD PROCESS
# ================================================================

def main():
    print("=" * 60)
    print("LOAD STAGING: Production Metrics")
    print("=" * 60)
    
    # Step 1: Read CSV
    print(f"\n[1/4] Reading CSV: {CSV_FILE}")
    
    if not os.path.exists(CSV_FILE):
        print(f"❌ ERROR: File not found: {CSV_FILE}")
        return
    
    df = pd.read_csv(CSV_FILE)
    print(f"✅ Loaded {len(df):,} rows, {len(df.columns)} columns")
    
    # Step 2: Parse vehicle_model into make + model
    print("\n[2/4] Parsing vehicle information...")
    
    # Split vehicle_model (format: "Make Model Year", e.g., "Jeep Wrangler 2024")
    df['vehicle_parts'] = df['vehicle_model'].str.rsplit(' ', n=1)
    df['vehicle_name'] = df['vehicle_parts'].str[0]  # "Jeep Wrangler"
    df['vehicle_year'] = df['vehicle_parts'].str[1]  # "2024"
    
    # Further split into make and model
    df['vehicle_name_parts'] = df['vehicle_name'].str.split(' ', n=1)
    df['vehicle_make'] = df['vehicle_name_parts'].str[0]  # "Jeep"
    df['vehicle_model_only'] = df['vehicle_name_parts'].str[1]  # "Wrangler"
    
    print(f"✅ Parsed vehicle information")
    print(f"    Sample: {df['vehicle_model'].iloc[0]} → Make: {df['vehicle_make'].iloc[0]}, Model: {df['vehicle_model_only'].iloc[0]}, Year: {df['vehicle_year'].iloc[0]}")
    
    # Step 3: Connect to PostgreSQL
    print("\n[3/4] Connecting to PostgreSQL...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ Connected to database")
    except Exception as e:
        print(f"❌ ERROR: Connection failed: {e}")
        return
    
    # Truncate staging table
    try:
        cursor.execute("TRUNCATE TABLE staging.stg_production_metrics;")
        conn.commit()
        print("✅ Truncated staging table")
    except Exception as e:
        print(f"❌ ERROR: Truncate failed: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return
    
    # Step 4: Load to staging
    print("\n[4/4] Loading to staging.stg_production_metrics...")
    
    records = []
    errors = 0
    
    for idx, row in df.iterrows():
        try:
            record = (
                # production_date (convert string to date)
                pd.to_datetime(row['date']).date(),
                
                # line_name (e.g., "Line_1")
                str(row['production_line']),
                
                # shift_name (e.g., "Morning")
                str(row['shift_name']),
                
                # vehicle_make (parsed from vehicle_model)
                str(row['vehicle_make']) if pd.notna(row['vehicle_make']) else 'Unknown',
                
                # vehicle_model (parsed from vehicle_model)
                str(row['vehicle_model_only']) if pd.notna(row['vehicle_model_only']) else 'Unknown',
                
                # vehicle_year (parsed from vehicle_model)
                int(row['vehicle_year']) if pd.notna(row['vehicle_year']) and str(row['vehicle_year']).isdigit() else 2024,
                
                # units_produced (actual_units)
                int(row['actual_units']),
                
                # units_target (expected_units)
                int(row['expected_units']),
                
                # defects (defect_count)
                int(row['defect_count']),
                
                # downtime_minutes
                float(row['downtime_min']),
                
                # cycle_time_seconds (convert minutes to seconds)
                float(row['actual_cycle_time_min']) * 60,
                
                # oee_percent (oee_score is already 0-100)
                float(row['oee_score'])
            )
            
            records.append(record)
            
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"    ⚠️  Warning: Skipping row {idx}: {e}")
            continue
    
    if errors > 5:
        print(f"    ⚠️  ... and {errors - 5} more errors")
    
    print(f"✅ Prepared {len(records):,} records for insert ({errors} rows skipped)")
    
    if len(records) == 0:
        print("❌ ERROR: No valid records to insert!")
        cursor.close()
        conn.close()
        return
    
    # Insert records
    insert_sql = """
        INSERT INTO staging.stg_production_metrics (
            production_date,
            line_name,
            shift_name,
            vehicle_make,
            vehicle_model,
            vehicle_year,
            units_produced,
            units_target,
            defects,
            downtime_minutes,
            cycle_time_seconds,
            oee_percent
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        execute_batch(cursor, insert_sql, records, page_size=500)
        conn.commit()
        print(f"✅ Inserted {len(records):,} rows")
    except Exception as e:
        conn.rollback()
        print(f"❌ ERROR: Insert failed: {e}")
        print(f"   Sample record: {records[0] if records else 'N/A'}")
        cursor.close()
        conn.close()
        return
    
    # Verify
    print("\n[5/5] Verifying load...")
    
    cursor.execute("SELECT COUNT(*) FROM staging.stg_production_metrics;")
    count = cursor.fetchone()[0]
    print(f"✅ Staging table row count: {count:,}")
    
    # Sample data
    cursor.execute("""
        SELECT 
            production_date,
            line_name,
            shift_name,
            vehicle_make,
            vehicle_model,
            units_produced,
            oee_percent
        FROM staging.stg_production_metrics
        ORDER BY production_date
        LIMIT 5;
    """)
    
    print(f"\n📊 Sample Staging Data (first 5 rows):")
    print(f"{'Date':<12} {'Line':<15} {'Shift':<12} {'Vehicle':<25} {'Units':<8} {'OEE %':<8}")
    print("-" * 90)
    
    for row in cursor.fetchall():
        vehicle = f"{row[3]} {row[4]}"
        print(f"{str(row[0]):<12} {row[1]:<15} {row[2]:<12} {vehicle:<25} {row[5]:<8} {row[6]:<8.2f}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 60)
    print("✅ STAGING LOAD COMPLETE - Production Metrics")
    print("=" * 60)


if __name__ == "__main__":
    main()
