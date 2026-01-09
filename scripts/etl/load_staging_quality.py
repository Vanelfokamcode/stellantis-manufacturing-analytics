#!/usr/bin/env python3
"""
================================================================
STELLANTIS MANUFACTURING ANALYTICS
Script: Load Staging - Quality Data
Author: Vanel
Date: January 2025
Description: Extract uci-secom.csv → staging.stg_quality (simplified)
Business: Preserve quality sensor data for ML (Week 5)
Note: Full 592 columns loaded in Week 5, simplified for Week 3
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
CSV_FILE = os.path.join(BASE_DIR, "data/raw/quality/uci-secom.csv")

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
    print("LOAD STAGING: Quality Data (Simplified)")
    print("=" * 60)
    
    # Step 1: Read CSV
    print(f"\n[1/4] Reading CSV: {CSV_FILE}")
    
    if not os.path.exists(CSV_FILE):
        print(f"❌ ERROR: File not found: {CSV_FILE}")
        return
    
    df = pd.read_csv(CSV_FILE)
    print(f"✅ Loaded {len(df):,} rows, {len(df.columns)} columns from CSV")
    print(f"    (Note: Full {len(df.columns)} columns will be used in Week 5 ML)")
    
    # Step 2: Connect to PostgreSQL
    print("\n[2/4] Connecting to PostgreSQL...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ Connected to database")
    except Exception as e:
        print(f"❌ ERROR: Connection failed: {e}")
        return
    
    # Step 3: Truncate staging
    print("\n[3/4] Truncating staging.stg_quality...")
    
    try:
        cursor.execute("TRUNCATE TABLE staging.stg_quality;")
        conn.commit()
        print("✅ Truncated staging table")
    except Exception as e:
        print(f"❌ ERROR: Truncate failed: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return
    
    # Step 4: Load simplified data
    print("\n[4/4] Loading to staging (simplified)...")
    
    # Get pass/fail column (last column)
    pass_fail_col = df.columns[-1]
    print(f"    Pass/Fail column: '{pass_fail_col}'")
    
    # Get first 3 numeric columns (any columns with numbers)
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
    
    # Take first 3 numeric columns (excluding pass/fail if it's last)
    sensor_cols = [col for col in numeric_cols if col != pass_fail_col][:3]
    
    if len(sensor_cols) < 3:
        # If less than 3, just use what we have
        print(f"    ⚠️  Only {len(sensor_cols)} sensor columns found")
        sensor_cols = sensor_cols + [None] * (3 - len(sensor_cols))
    
    print(f"    Sensor columns: {sensor_cols[:3]}")
    
    records = []
    for idx, row in df.iterrows():
        try:
            record = (
                # pass_fail
                int(row[pass_fail_col]) if pd.notna(row[pass_fail_col]) else None,
                
                # sensor_1
                float(row[sensor_cols[0]]) if sensor_cols[0] and pd.notna(row[sensor_cols[0]]) else None,
                
                # sensor_2
                float(row[sensor_cols[1]]) if sensor_cols[1] and pd.notna(row[sensor_cols[1]]) else None,
                
                # sensor_3
                float(row[sensor_cols[2]]) if sensor_cols[2] and pd.notna(row[sensor_cols[2]]) else None
            )
            records.append(record)
        except Exception as e:
            print(f"    ⚠️  Warning: Skipping row {idx}: {e}")
            continue
    
    insert_sql = """
        INSERT INTO staging.stg_quality (
            pass_fail, sensor_1, sensor_2, sensor_3
        ) VALUES (%s, %s, %s, %s)
    """
    
    try:
        execute_batch(cursor, insert_sql, records, page_size=500)
        conn.commit()
        print(f"✅ Inserted {len(records):,} rows")
    except Exception as e:
        conn.rollback()
        print(f"❌ ERROR: Insert failed: {e}")
        cursor.close()
        conn.close()
        return
    
    # Verify
    cursor.execute("SELECT COUNT(*) FROM staging.stg_quality;")
    count = cursor.fetchone()[0]
    print(f"✅ Staging table row count: {count:,}")
    
    # Pass/Fail stats
    cursor.execute("""
        SELECT 
            pass_fail,
            COUNT(*) as count
        FROM staging.stg_quality
        GROUP BY pass_fail
        ORDER BY pass_fail;
    """)
    
    print(f"\n📊 Pass/Fail Distribution:")
    for row in cursor.fetchall():
        if row[0] is None:
            status = "Unknown"
        elif row[0] == -1:
            status = "PASS"
        elif row[0] == 1:
            status = "FAIL"
        else:
            status = f"Code {row[0]}"
        print(f"    {status}: {row[1]:,} records")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 60)
    print("✅ STAGING LOAD COMPLETE - Quality Data")
    print("=" * 60)
    print("\n💡 Note: This is simplified for Week 3.")
    print("   Full 592 sensor columns will be used in Week 5 ML model.")


if __name__ == "__main__":
    main()
