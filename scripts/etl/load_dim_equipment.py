#!/usr/bin/env python3
"""
================================================================
STELLANTIS MANUFACTURING ANALYTICS
Script: Load dim_equipment from maintenance CSV
Author: Vanel
Date: January 2025
Description: ETL pipeline to load equipment catalog into PostgreSQL
Business: Enable predictive maintenance analytics
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
    'password': 'postgres'  # Change if different
}

# ================================================================
# MAIN ETL PROCESS
# ================================================================

def main():
    print("=" * 60)
    print("LOAD dim_equipment - ETL Pipeline")
    print("=" * 60)
    
    # Step 1: Read CSV
    print(f"\n[1/5] Reading CSV: {CSV_FILE}")
    
    if not os.path.exists(CSV_FILE):
        print(f"❌ ERROR: File not found: {CSV_FILE}")
        return
    
    df = pd.read_csv(CSV_FILE)
    print(f"✅ Loaded {len(df):,} rows, {len(df.columns)} columns")
    print(f"    Columns: {list(df.columns)}")
    
    # Step 2: Aggregate by equipment (UDI)
    print("\n[2/5] Aggregating by equipment...")
    
    # Group by UDI (equipment ID) and calculate stats
    equipment_agg = df.groupby('UDI').agg({
        'Type': 'first',  # Equipment type (L/M/H)
        'Air temperature [K]': 'mean',
        'Process temperature [K]': 'mean',
        'Rotational speed [rpm]': 'mean',
        'Torque [Nm]': 'mean',
        'Tool wear [min]': 'mean',
        'Machine failure': 'mean'  # Failure rate (proportion of failures)
    }).reset_index()
    
    # Rename columns for clarity
    equipment_agg.columns = [
        'equipment_id',
        'equipment_type',
        'air_temp_avg',
        'process_temp_avg',
        'rotational_speed_avg',
        'torque_avg',
        'tool_wear_avg',
        'failure_rate'
    ]
    
    # Convert failure rate to percentage
    equipment_agg['failure_rate_percent'] = equipment_agg['failure_rate'] * 100
    equipment_agg = equipment_agg.drop(columns=['failure_rate'])
    
    # Round values to 2 decimals
    numeric_cols = [
        'air_temp_avg', 'process_temp_avg', 'rotational_speed_avg',
        'torque_avg', 'tool_wear_avg', 'failure_rate_percent'
    ]
    equipment_agg[numeric_cols] = equipment_agg[numeric_cols].round(2)
    
    print(f"✅ Aggregated to {len(equipment_agg):,} unique equipment")
    
    # Step 3: Validate
    print("\n[3/5] Validating data...")
    
    print(f"    Equipment types: {equipment_agg['equipment_type'].unique()}")
    print(f"    Avg failure rate: {equipment_agg['failure_rate_percent'].mean():.2f}%")
    print(f"    Max failure rate: {equipment_agg['failure_rate_percent'].max():.2f}%")
    print(f"    Min failure rate: {equipment_agg['failure_rate_percent'].min():.2f}%")
    
    # Check for nulls
    null_counts = equipment_agg.isnull().sum()
    if null_counts.sum() > 0:
        print("⚠️  WARNING: Null values found:")
        print(null_counts[null_counts > 0])
    else:
        print("✅ No null values")
    
    print("✅ Validation passed")
    
    # Step 4: Connect to PostgreSQL
    print("\n[4/5] Connecting to PostgreSQL...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ Connected to database")
    except Exception as e:
        print(f"❌ ERROR: Connection failed: {e}")
        return
    
    # Step 5: Load data
    print("\n[5/5] Loading to PostgreSQL...")
    
    # Truncate table
    try:
        cursor.execute("TRUNCATE TABLE warehouse.dim_equipment RESTART IDENTITY CASCADE;")
        conn.commit()
        print("✅ Truncated existing data")
    except Exception as e:
        print(f"❌ ERROR: Truncate failed: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return
    
    # Prepare records
    records = []
    for _, row in equipment_agg.iterrows():
        records.append((
            str(row['equipment_id']),
            str(row['equipment_type']),
            float(row['air_temp_avg']),
            float(row['rotational_speed_avg']),
            float(row['torque_avg']),
            float(row['tool_wear_avg']),
            float(row['failure_rate_percent'])
        ))
    
    # Insert
    insert_sql = """
        INSERT INTO warehouse.dim_equipment (
            equipment_id,
            equipment_type,
            air_temp_avg,
            rotational_speed_avg,
            torque_avg,
            tool_wear_avg,
            failure_rate_percent
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        execute_batch(cursor, insert_sql, records, page_size=1000)
        conn.commit()
        print(f"✅ Inserted {len(records):,} equipment")
    except Exception as e:
        conn.rollback()
        print(f"❌ ERROR: Insert failed: {e}")
        cursor.close()
        conn.close()
        return
    
    # Verify
    print("\n[6/6] Verifying...")
    
    cursor.execute("SELECT COUNT(*) FROM warehouse.dim_equipment;")
    count = cursor.fetchone()[0]
    print(f"✅ Total rows: {count:,}")
    
    # Stats
    cursor.execute("""
        SELECT 
            equipment_type,
            COUNT(*) as count,
            ROUND(AVG(failure_rate_percent), 2) as avg_failure_rate,
            ROUND(MAX(failure_rate_percent), 2) as max_failure_rate
        FROM warehouse.dim_equipment
        GROUP BY equipment_type
        ORDER BY equipment_type;
    """)
    
    print(f"\n📊 Summary by Equipment Type:")
    print(f"{'Type':<10} {'Count':<10} {'Avg Failure %':<15} {'Max Failure %':<15}")
    print("-" * 50)
    
    for row in cursor.fetchall():
        print(f"{row[0]:<10} {row[1]:<10} {row[2]:<15} {row[3]:<15}")
    
    # High-risk equipment
    cursor.execute("""
        SELECT 
            equipment_id,
            equipment_type,
            failure_rate_percent,
            air_temp_avg,
            tool_wear_avg
        FROM warehouse.dim_equipment
        WHERE failure_rate_percent > 5.0
        ORDER BY failure_rate_percent DESC
        LIMIT 10;
    """)
    
    high_risk = cursor.fetchall()
    if high_risk:
        print(f"\n⚠️  High-Risk Equipment (failure rate > 5%):")
        print(f"{'Equipment ID':<15} {'Type':<6} {'Failure %':<12} {'Temp (K)':<10} {'Tool Wear':<10}")
        print("-" * 65)
        
        for row in high_risk:
            print(f"{row[0]:<15} {row[1]:<6} {row[2]:<12.2f} {row[3]:<10.2f} {row[4]:<10.2f}")
    
    # Sample equipment
    cursor.execute("""
        SELECT 
            equipment_id,
            equipment_type,
            air_temp_avg,
            rotational_speed_avg,
            torque_avg,
            failure_rate_percent
        FROM warehouse.dim_equipment
        ORDER BY RANDOM()
        LIMIT 5;
    """)
    
    print(f"\n🎲 Sample Equipment:")
    print(f"{'ID':<15} {'Type':<6} {'Temp':<10} {'RPM':<10} {'Torque':<10} {'Fail %':<10}")
    print("-" * 65)
    
    for row in cursor.fetchall():
        print(f"{row[0]:<15} {row[1]:<6} {row[2]:<10.2f} {row[3]:<10.2f} {row[4]:<10.2f} {row[5]:<10.2f}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 60)
    print("✅ ETL COMPLETE!")
    print("=" * 60)
    print("\n💡 Business Insight:")
    print("   Equipment with >5% failure rate should be scheduled")
    print("   for predictive maintenance to reduce downtime costs.")


if __name__ == "__main__":
    main()
