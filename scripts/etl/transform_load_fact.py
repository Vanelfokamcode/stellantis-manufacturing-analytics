#!/usr/bin/env python3
"""
================================================================
STELLANTIS MANUFACTURING ANALYTICS
Script: Transform & Load Fact Table
Author: Vanel
Date: January 2025
Description: Transform staging → warehouse.fact_production_metrics
Business: Enable analytics queries with clean, validated data
================================================================
"""

import psycopg2
from psycopg2.extras import execute_batch
import sys

# ================================================================
# CONFIGURATION
# ================================================================

DB_CONFIG = {
    'host': 'localhost',
    'database': 'stellantis_manufacturing',
    'user': 'postgres',
    'password': 'postgres'
}

# ================================================================
# LOOKUP FUNCTIONS (Dimension Key Resolution)
# ================================================================

def get_line_key(cursor, line_name):
    """Lookup production line key by name"""
    cursor.execute(
        "SELECT line_key FROM warehouse.dim_production_line WHERE line_name = %s",
        (line_name,)
    )
    result = cursor.fetchone()
    if not result:
        raise ValueError(f"Production line not found: {line_name}")
    return result[0]


def get_shift_key(cursor, shift_name):
    """Lookup shift key by name"""
    cursor.execute(
        "SELECT shift_key FROM warehouse.dim_shift WHERE shift_name = %s",
        (shift_name,)
    )
    result = cursor.fetchone()
    if not result:
        raise ValueError(f"Shift not found: {shift_name}")
    return result[0]


def get_vehicle_key(cursor, make, model, year):
    """Lookup vehicle key by make, model, year"""
    cursor.execute("""
        SELECT vehicle_key 
        FROM warehouse.dim_vehicle 
        WHERE make = %s AND model = %s AND year = %s
        LIMIT 1
    """, (make, model, year))
    
    result = cursor.fetchone()
    if not result:
        # Try without year (less strict)
        cursor.execute("""
            SELECT vehicle_key 
            FROM warehouse.dim_vehicle 
            WHERE make = %s AND model = %s
            ORDER BY year DESC
            LIMIT 1
        """, (make, model))
        
        result = cursor.fetchone()
        if not result:
            raise ValueError(f"Vehicle not found: {make} {model} {year}")
    
    return result[0]


# ================================================================
# VALIDATION FUNCTIONS
# ================================================================

def validate_oee(oee_percent):
    """Validate OEE is between 0 and 100"""
    if oee_percent < 0 or oee_percent > 100:
        raise ValueError(f"Invalid OEE: {oee_percent} (must be 0-100)")
    return True


def validate_defects(defects, units_produced):
    """Validate defects don't exceed production"""
    if defects > units_produced:
        raise ValueError(f"Defects ({defects}) > Production ({units_produced})")
    return True


def validate_downtime(downtime_minutes):
    """Validate downtime is non-negative"""
    if downtime_minutes < 0:
        raise ValueError(f"Negative downtime: {downtime_minutes}")
    return True


# ================================================================
# MAIN TRANSFORMATION PROCESS
# ================================================================

def main():
    print("=" * 70)
    print("TRANSFORM & LOAD: Fact Production Metrics")
    print("=" * 70)
    
    # Connect to database
    print("\n[1/6] Connecting to PostgreSQL...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ Connected to database")
    except Exception as e:
        print(f"❌ ERROR: Connection failed: {e}")
        return
    
    # Check staging data exists
    print("\n[2/6] Checking staging data...")
    
    cursor.execute("SELECT COUNT(*) FROM staging.stg_production_metrics;")
    staging_count = cursor.fetchone()[0]
    
    if staging_count == 0:
        print("❌ ERROR: Staging table is empty! Run load_staging_production.py first.")
        cursor.close()
        conn.close()
        return
    
    print(f"✅ Staging has {staging_count:,} rows")
    
    # Truncate fact table
    print("\n[3/6] Truncating fact table...")
    
    try:
        cursor.execute("TRUNCATE TABLE warehouse.fact_production_metrics RESTART IDENTITY CASCADE;")
        conn.commit()
        print("✅ Fact table truncated (fresh load)")
    except Exception as e:
        print(f"❌ ERROR: Truncate failed: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return
    
    # Transform and load
    print("\n[4/6] Transforming staging data...")
    
    # Fetch staging data
    cursor.execute("""
        SELECT 
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
        FROM staging.stg_production_metrics
        ORDER BY production_date, line_name, shift_name
    """)
    
    staging_rows = cursor.fetchall()
    print(f"✅ Fetched {len(staging_rows):,} rows from staging")
    
    # Transform each row
    print("\n[5/6] Loading to warehouse...")
    
    fact_records = []
    errors = []
    
    for idx, row in enumerate(staging_rows):
        try:
            (production_date, line_name, shift_name, vehicle_make, 
             vehicle_model, vehicle_year, units_produced, units_target,
             defects, downtime_minutes, cycle_time_seconds, oee_percent) = row
            
            # Lookup dimension keys
            line_key = get_line_key(cursor, line_name)
            shift_key = get_shift_key(cursor, shift_name)
            vehicle_key = get_vehicle_key(cursor, vehicle_make, vehicle_model, vehicle_year)
            
            # Validate business rules
            validate_oee(oee_percent)
            validate_defects(defects, units_produced)
            validate_downtime(downtime_minutes)
            
            # Create fact record
            fact_record = (
                production_date,      # date_key
                line_key,            # line_key (FK)
                vehicle_key,         # vehicle_key (FK)
                shift_key,           # shift_key (FK)
                None,                # equipment_key (nullable for now)
                units_produced,
                units_target,
                defects,
                downtime_minutes,
                cycle_time_seconds,
                oee_percent
            )
            
            fact_records.append(fact_record)
            
        except Exception as e:
            error_msg = f"Row {idx}: {e} | Data: {line_name}, {shift_name}, {vehicle_make} {vehicle_model}"
            errors.append(error_msg)
            if len(errors) <= 5:
                print(f"    ⚠️  {error_msg}")
            continue
    
    if len(errors) > 5:
        print(f"    ⚠️  ... and {len(errors) - 5} more errors")
    
    print(f"\n✅ Transformed {len(fact_records):,} records ({len(errors)} errors)")
    
    if len(fact_records) == 0:
        print("❌ ERROR: No valid records to insert!")
        if errors:
            print("\nErrors encountered:")
            for err in errors[:10]:
                print(f"  - {err}")
        cursor.close()
        conn.close()
        return
    
    # Batch insert to fact table
    insert_sql = """
        INSERT INTO warehouse.fact_production_metrics (
            date_key,
            line_key,
            vehicle_key,
            shift_key,
            equipment_key,
            units_produced,
            units_target,
            defects,
            downtime_minutes,
            cycle_time_seconds,
            oee_percent
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        execute_batch(cursor, insert_sql, fact_records, page_size=500)
        conn.commit()
        print(f"✅ Inserted {len(fact_records):,} rows into fact table")
    except Exception as e:
        conn.rollback()
        print(f"❌ ERROR: Insert failed: {e}")
        print(f"   Sample record: {fact_records[0] if fact_records else 'N/A'}")
        cursor.close()
        conn.close()
        return
    
    # Verify
    print("\n[6/6] Verifying fact table...")
    
    cursor.execute("SELECT COUNT(*) FROM warehouse.fact_production_metrics;")
    fact_count = cursor.fetchone()[0]
    print(f"✅ Fact table row count: {fact_count:,}")
    
    # Validation queries
    print("\n📊 Data Quality Checks:")
    
    # Check 1: No orphan foreign keys
    cursor.execute("""
        SELECT COUNT(*) 
        FROM warehouse.fact_production_metrics f
        LEFT JOIN warehouse.dim_date d ON f.date_key = d.date
        WHERE d.date IS NULL
    """)
    orphan_dates = cursor.fetchone()[0]
    print(f"    Orphan dates: {orphan_dates} {'✅' if orphan_dates == 0 else '❌'}")
    
    # Check 2: OEE range
    cursor.execute("""
        SELECT COUNT(*) 
        FROM warehouse.fact_production_metrics
        WHERE oee_percent NOT BETWEEN 0 AND 100
    """)
    invalid_oee = cursor.fetchone()[0]
    print(f"    Invalid OEE values: {invalid_oee} {'✅' if invalid_oee == 0 else '❌'}")
    
    # Check 3: Defects vs production
    cursor.execute("""
        SELECT COUNT(*) 
        FROM warehouse.fact_production_metrics
        WHERE defects > units_produced
    """)
    invalid_defects = cursor.fetchone()[0]
    print(f"    Defects > Production: {invalid_defects} {'✅' if invalid_defects == 0 else '❌'}")
    
    # Check 4: Duplicates (grain = date + line + shift)
    cursor.execute("""
        SELECT COUNT(*) 
        FROM (
            SELECT date_key, line_key, shift_key, COUNT(*)
            FROM warehouse.fact_production_metrics
            GROUP BY date_key, line_key, shift_key
            HAVING COUNT(*) > 1
        ) duplicates
    """)
    duplicates = cursor.fetchone()[0]
    print(f"    Duplicate records: {duplicates} {'✅' if duplicates == 0 else '❌'}")
    
    # Sample data
    print("\n📈 Sample Fact Data (first 5 rows):")
    cursor.execute("""
        SELECT 
            f.date_key,
            pl.line_name,
            s.shift_name,
            v.make || ' ' || v.model as vehicle,
            f.units_produced,
            f.oee_percent
        FROM warehouse.fact_production_metrics f
        JOIN warehouse.dim_production_line pl ON f.line_key = pl.line_key
        JOIN warehouse.dim_shift s ON f.shift_key = s.shift_key
        JOIN warehouse.dim_vehicle v ON f.vehicle_key = v.vehicle_key
        ORDER BY f.date_key, pl.line_name
        LIMIT 5;
    """)
    
    print(f"{'Date':<12} {'Line':<15} {'Shift':<12} {'Vehicle':<25} {'Units':<8} {'OEE %':<8}")
    print("-" * 90)
    
    for row in cursor.fetchall():
        print(f"{str(row[0]):<12} {row[1]:<15} {row[2]:<12} {row[3]:<25} {row[4]:<8} {row[5]:<8.2f}")
    
    # Summary stats
    print("\n📊 Summary Statistics:")
    cursor.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT date_key) as unique_dates,
            COUNT(DISTINCT line_key) as unique_lines,
            COUNT(DISTINCT shift_key) as unique_shifts,
            ROUND(AVG(oee_percent), 2) as avg_oee,
            SUM(units_produced) as total_units
        FROM warehouse.fact_production_metrics
    """)
    
    stats = cursor.fetchone()
    print(f"    Total records: {stats[0]:,}")
    print(f"    Unique dates: {stats[1]}")
    print(f"    Unique lines: {stats[2]}")
    print(f"    Unique shifts: {stats[3]}")
    print(f"    Average OEE: {stats[4]}%")
    print(f"    Total units produced: {stats[5]:,}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 70)
    print("✅ TRANSFORMATION COMPLETE!")
    print("=" * 70)
    print("\n💡 Fact table is now ready for analytics queries!")
    print("   Try running business queries to see insights.")
    
    if errors:
        print(f"\n⚠️  Note: {len(errors)} rows were skipped due to errors.")
        print("   Check error log above for details.")


if __name__ == "__main__":
    main()
