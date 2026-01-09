#!/usr/bin/env python3
"""
================================================================
STELLANTIS MANUFACTURING ANALYTICS
Script: Load Staging - Maintenance Data
Author: Vanel
Date: January 2025
Description: Extract ai4i2020.csv → staging.stg_maintenance
Business: Preserve raw equipment data for predictive maintenance
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
CSV_FILE = os.path.join(BASE_DIR, "data/raw/maintenance/ai4i2020.csv")

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
    print("LOAD STAGING: Maintenance Data")
    print("=" * 60)
    
    # Step 1: Read CSV
    print(f"\n[1/4] Reading CSV: {CSV_FILE}")
    
    if not os.path.exists(CSV_FILE):
        print(f"❌ ERROR: File not found: {CSV_FILE}")
        return
    
    df = pd.read_csv(CSV_FILE)
    print(f"✅ Loaded {len(df):,} rows from CSV")
    
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
    print("\n[3/4] Truncating staging.stg_maintenance...")
    
    try:
        cursor.execute("TRUNCATE TABLE staging.stg_maintenance;")
        conn.commit()
        print("✅ Truncated staging table")
    except Exception as e:
        print(f"❌ ERROR: Truncate failed: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return
    
    # Step 4: Load data
    print("\n[4/4] Loading to staging...")
    
    records = []
    for _, row in df.iterrows():
        records.append((
            str(row['UDI']),
            str(row['Product ID']) if pd.notna(row.get('Product ID')) else None,
            str(row['Type']),
            float(row['Air temperature [K]']),
            float(row['Process temperature [K]']),
            float(row['Rotational speed [rpm]']),
            float(row['Torque [Nm]']),
            float(row['Tool wear [min]']),
            int(row['Machine failure']),
            int(row['TWF']),
            int(row['HDF']),
            int(row['PWF']),
            int(row['OSF']),
            int(row['RNF'])
        ))
    
    insert_sql = """
        INSERT INTO staging.stg_maintenance (
            udi, product_id, type,
            air_temperature_k, process_temperature_k,
            rotational_speed_rpm, torque_nm, tool_wear_min,
            machine_failure, twf, hdf, pwf, osf, rnf
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        execute_batch(cursor, insert_sql, records, page_size=1000)
        conn.commit()
        print(f"✅ Inserted {len(records):,} rows")
    except Exception as e:
        conn.rollback()
        print(f"❌ ERROR: Insert failed: {e}")
        cursor.close()
        conn.close()
        return
    
    # Verify
    cursor.execute("SELECT COUNT(*) FROM staging.stg_maintenance;")
    count = cursor.fetchone()[0]
    print(f"✅ Staging table row count: {count:,}")
    
    # Sample
    cursor.execute("""
        SELECT udi, type, air_temperature_k, rotational_speed_rpm, machine_failure
        FROM staging.stg_maintenance
        LIMIT 5;
    """)
    
    print(f"\n📊 Sample Staging Data:")
    for row in cursor.fetchall():
        print(f"    UDI: {row[0]}, Type: {row[1]}, Temp: {row[2]:.1f}K, RPM: {row[3]:.0f}, Failure: {row[4]}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 60)
    print("✅ STAGING LOAD COMPLETE - Maintenance Data")
    print("=" * 60)


if __name__ == "__main__":
    main()
