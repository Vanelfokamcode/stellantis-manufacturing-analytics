#!/usr/bin/env python3
"""
================================================================
STELLANTIS MANUFACTURING ANALYTICS
Script: Load dim_vehicle from CSV
Author: Vanel
Date: January 2025
Description: ETL pipeline to load vehicle catalog into PostgreSQL
Business: Enable vehicle-based analytics (defects by model, OEE by make)
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
CSV_FILE = os.path.join(BASE_DIR, "data/raw/vehicles/data.csv")

DB_CONFIG = {
    'host': 'localhost',
    'database': 'stellantis_manufacturing',
    'user': 'postgres',
    'password': 'postgres'  # Change if different
}

# ================================================================
# HELPER FUNCTIONS
# ================================================================

def derive_body_style(model, vehicle_style):
    """Derive body style from model name and vehicle_style column"""
    model_lower = str(model).lower()
    style_lower = str(vehicle_style).lower() if pd.notna(vehicle_style) else ''
    
    # Use Vehicle Style column first (from CSV)
    if 'suv' in style_lower or 'sport utility' in style_lower:
        return 'SUV'
    if 'truck' in style_lower or 'pickup' in style_lower:
        return 'Truck'
    if 'van' in style_lower or 'passenger van' in style_lower:
        return 'Van'
    if 'coupe' in style_lower:
        return 'Coupe'
    if 'sedan' in style_lower:
        return 'Sedan'
    
    # Fallback to model name keywords
    if any(word in model_lower for word in ['wrangler', 'cherokee', 'explorer', 'tahoe']):
        return 'SUV'
    if any(word in model_lower for word in ['ram', 'f-150', 'silverado']):
        return 'Truck'
    if any(word in model_lower for word in ['caravan', 'odyssey']):
        return 'Van'
    if any(word in model_lower for word in ['challenger', 'mustang', 'camaro']):
        return 'Coupe'
    
    return 'Sedan'  # Default


def derive_fuel_type(engine_fuel_type):
    """Derive standardized fuel type from Engine Fuel Type column"""
    if pd.isna(engine_fuel_type):
        return 'Gasoline'  # Default
    
    fuel_lower = str(engine_fuel_type).lower()
    
    if 'electric' in fuel_lower or 'battery' in fuel_lower:
        return 'Electric'
    if 'hybrid' in fuel_lower:
        return 'Hybrid'
    if 'diesel' in fuel_lower:
        return 'Diesel'
    
    return 'Gasoline'  # Default (includes premium, regular, flex fuel)


# ================================================================
# MAIN ETL PROCESS
# ================================================================

def main():
    print("=" * 60)
    print("LOAD dim_vehicle - ETL Pipeline")
    print("=" * 60)
    
    # Step 1: Read CSV
    print(f"\n[1/5] Reading CSV: {CSV_FILE}")
    
    if not os.path.exists(CSV_FILE):
        print(f"❌ ERROR: File not found: {CSV_FILE}")
        return
    
    df = pd.read_csv(CSV_FILE)
    print(f"✅ Loaded {len(df):,} rows, {len(df.columns)} columns")
    
    # Step 2: Select and rename columns
    print("\n[2/5] Selecting relevant columns...")
    
    # Keep only columns we need
    df_clean = df[[
        'Make',
        'Model', 
        'Year',
        'MSRP',
        'Engine Cylinders',
        'Engine Fuel Type',
        'Vehicle Style'
    ]].copy()
    
    # Rename to match our schema (lowercase)
    df_clean.columns = [
        'make',
        'model',
        'year',
        'msrp',
        'engine_cylinders',
        'engine_fuel_type',
        'vehicle_style'
    ]
    
    print(f"✅ Selected columns: {list(df_clean.columns)}")
    
    # Step 3: Transform data
    print("\n[3/5] Transforming data...")
    
    # Derive body_style (using vehicle_style from CSV)
    df_clean['body_style'] = df_clean.apply(
        lambda row: derive_body_style(row['model'], row['vehicle_style']), 
        axis=1
    )
    
    # Derive fuel_type (from engine_fuel_type)
    df_clean['fuel_type'] = df_clean['engine_fuel_type'].apply(derive_fuel_type)
    
    # All current models
    df_clean['is_current_model'] = True
    
    # Drop the temp columns we don't need in DB
    df_clean = df_clean.drop(columns=['engine_fuel_type', 'vehicle_style'])
    
    # Remove duplicates
    before = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=['make', 'model', 'year'], keep='first')
    after = len(df_clean)
    
    if before > after:
        print(f"✅ Removed {before - after:,} duplicates")
    
    print(f"✅ Derived body_style and fuel_type")
    
    # Step 4: Validate
    print("\n[4/5] Validating data...")
    
    missing_make = df_clean['make'].isna().sum()
    missing_model = df_clean['model'].isna().sum()
    
    if missing_make > 0 or missing_model > 0:
        print(f"❌ ERROR: Missing critical data (make: {missing_make}, model: {missing_model})")
        return
    
    print(f"    Year range: {df_clean['year'].min()} - {df_clean['year'].max()}")
    print(f"    Unique makes: {df_clean['make'].nunique()}")
    print(f"    Unique models: {df_clean['model'].nunique()}")
    print("✅ Validation passed")
    
    # Step 5: Load to PostgreSQL
    print("\n[5/5] Loading to PostgreSQL...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ Connected to database")
    except Exception as e:
        print(f"❌ ERROR: Connection failed: {e}")
        return
    
    # Truncate table
    try:
        cursor.execute("TRUNCATE TABLE warehouse.dim_vehicle RESTART IDENTITY CASCADE;")
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
    for _, row in df_clean.iterrows():
        records.append((
            str(row['make']),
            str(row['model']),
            int(row['year']),
            row['body_style'],
            float(row['msrp']) if pd.notna(row['msrp']) else None,
            int(row['engine_cylinders']) if pd.notna(row['engine_cylinders']) else None,
            row['fuel_type'],
            row['is_current_model']
        ))
    
    # Insert
    insert_sql = """
        INSERT INTO warehouse.dim_vehicle (
            make, model, year, body_style, msrp, 
            engine_cylinders, fuel_type, is_current_model
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        execute_batch(cursor, insert_sql, records, page_size=1000)
        conn.commit()
        print(f"✅ Inserted {len(records):,} vehicles")
    except Exception as e:
        conn.rollback()
        print(f"❌ ERROR: Insert failed: {e}")
        cursor.close()
        conn.close()
        return
    
    # Verify
    print("\n[6/6] Verifying...")
    
    cursor.execute("SELECT COUNT(*) FROM warehouse.dim_vehicle;")
    count = cursor.fetchone()[0]
    print(f"✅ Total rows: {count:,}")
    
    # Stats
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT make) as makes,
            COUNT(DISTINCT body_style) as body_styles,
            COUNT(DISTINCT fuel_type) as fuel_types,
            MIN(year) as min_year,
            MAX(year) as max_year,
            ROUND(AVG(msrp), 2) as avg_msrp
        FROM warehouse.dim_vehicle;
    """)
    
    stats = cursor.fetchone()
    print(f"\n📊 Summary:")
    print(f"    Unique makes: {stats[0]}")
    print(f"    Body styles: {stats[1]}")
    print(f"    Fuel types: {stats[2]}")
    print(f"    Year range: {stats[3]} - {stats[4]}")
    print(f"    Avg MSRP: ${stats[5]:,.2f}" if stats[5] else "    Avg MSRP: N/A")
    
    # Samples
    cursor.execute("""
        SELECT make, model, year, body_style, fuel_type, msrp
        FROM warehouse.dim_vehicle
        ORDER BY RANDOM()
        LIMIT 5;
    """)
    
    print(f"\n🎲 Sample Vehicles:")
    print(f"{'Make':<15} {'Model':<25} {'Year':<6} {'Body':<10} {'Fuel':<12} {'MSRP':<12}")
    print("-" * 90)
    
    for row in cursor.fetchall():
        msrp_str = f"${row[5]:,.0f}" if row[5] else "N/A"
        print(f"{row[0]:<15} {row[1]:<25} {row[2]:<6} {row[3]:<10} {row[4]:<12} {msrp_str:<12}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 60)
    print("✅ ETL COMPLETE!")
    print("=" * 60)


if __name__ == "__main__":
    main()
