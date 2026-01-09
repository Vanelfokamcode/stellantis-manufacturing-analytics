#!/usr/bin/env python3
"""
================================================================
STELLANTIS MANUFACTURING ANALYTICS
Script: Load dim_quality_defect
Author: Vanel
Date: January 2025
Description: Create standard automotive quality defect catalog
Business: Enable defect cost analysis and quality prioritization
================================================================
"""

import psycopg2
from psycopg2.extras import execute_batch

# ================================================================
# CONFIGURATION
# ================================================================

DB_CONFIG = {
    'host': 'localhost',
    'database': 'stellantis_manufacturing',
    'user': 'postgres',
    'password': 'postgres'  # Change if different
}

# ================================================================
# DEFECT CATALOG (Industry Standard)
# ================================================================

# Based on automotive quality standards (SAE, AIAG)
DEFECT_CATALOG = [
    # PAINT DEFECTS (Category 1)
    ('D001', 'Paint', 'Orange peel surface finish', 2, 15, 120, False),
    ('D002', 'Paint', 'Paint runs or sags', 3, 30, 250, False),
    ('D003', 'Paint', 'Dirt inclusion in paint', 2, 20, 180, False),
    ('D004', 'Paint', 'Color mismatch between panels', 3, 45, 400, False),
    ('D005', 'Paint', 'Paint thickness variation', 2, 25, 200, False),
    ('D006', 'Paint', 'Overspray', 1, 10, 80, False),
    ('D007', 'Paint', 'Fish eyes in paint', 2, 20, 150, False),
    ('D008', 'Paint', 'Clear coat delamination', 4, 60, 500, False),
    
    # BODY DEFECTS (Category 2)
    ('D009', 'Body', 'Dent in body panel', 3, 35, 300, False),
    ('D010', 'Body', 'Scratch on exterior', 2, 25, 200, False),
    ('D011', 'Body', 'Panel gap misalignment', 3, 40, 350, False),
    ('D012', 'Body', 'Door fitment issue', 4, 50, 450, True),
    ('D013', 'Body', 'Hood alignment', 3, 35, 300, False),
    ('D014', 'Body', 'Trunk/hatch misalignment', 3, 40, 320, False),
    ('D015', 'Body', 'Spot weld defect', 4, 45, 400, True),
    ('D016', 'Body', 'Rust or corrosion', 3, 30, 280, False),
    
    # ELECTRICAL DEFECTS (Category 3)
    ('D017', 'Electrical', 'Faulty wiring harness', 4, 60, 600, True),
    ('D018', 'Electrical', 'Sensor malfunction', 4, 45, 500, True),
    ('D019', 'Electrical', 'Connector not seated', 3, 15, 150, True),
    ('D020', 'Electrical', 'Short circuit', 5, 90, 800, True),
    ('D021', 'Electrical', 'Battery connection issue', 4, 30, 350, True),
    ('D022', 'Electrical', 'Lighting system fault', 3, 40, 400, True),
    ('D023', 'Electrical', 'Infotainment system error', 3, 50, 450, False),
    ('D024', 'Electrical', 'Climate control malfunction', 3, 45, 420, False),
    
    # ASSEMBLY DEFECTS (Category 4)
    ('D025', 'Assembly', 'Loose bolts/fasteners', 4, 20, 200, True),
    ('D026', 'Assembly', 'Incorrect torque specification', 5, 30, 350, True),
    ('D027', 'Assembly', 'Missing component', 5, 40, 500, True),
    ('D028', 'Assembly', 'Reversed component installation', 4, 35, 400, True),
    ('D029', 'Assembly', 'Cross-threaded fastener', 3, 25, 280, True),
    ('D030', 'Assembly', 'Incorrect part installed', 5, 50, 600, True),
    ('D031', 'Assembly', 'Seal not properly seated', 4, 30, 350, True),
    ('D032', 'Assembly', 'Clip or retainer missing', 3, 15, 180, False),
    
    # MECHANICAL DEFECTS (Category 5)
    ('D033', 'Mechanical', 'Engine noise/vibration', 5, 120, 1200, True),
    ('D034', 'Mechanical', 'Transmission issue', 5, 150, 1500, True),
    ('D035', 'Mechanical', 'Brake system defect', 5, 80, 900, True),
    ('D036', 'Mechanical', 'Suspension component fault', 4, 90, 850, True),
    ('D037', 'Mechanical', 'Steering malfunction', 5, 100, 1000, True),
    ('D038', 'Mechanical', 'Exhaust leak', 3, 60, 500, True),
    ('D039', 'Mechanical', 'Cooling system leak', 4, 70, 650, True),
    ('D040', 'Mechanical', 'Oil leak', 4, 60, 550, True),
    
    # INTERIOR DEFECTS (Category 6)
    ('D041', 'Interior', 'Seat trim defect', 2, 30, 250, False),
    ('D042', 'Interior', 'Dashboard gap/rattle', 2, 25, 200, False),
    ('D043', 'Interior', 'Door panel loose', 2, 20, 180, False),
    ('D044', 'Interior', 'Headliner sag', 2, 35, 300, False),
    ('D045', 'Interior', 'Carpet misalignment', 1, 15, 120, False),
    ('D046', 'Interior', 'Console lid malfunction', 2, 20, 180, False),
    ('D047', 'Interior', 'Stitching defect', 1, 10, 100, False),
    ('D048', 'Interior', 'Climate vent misaligned', 1, 12, 110, False),
    
    # FUNCTIONAL DEFECTS (Category 7)
    ('D049', 'Functional', 'Software calibration error', 3, 60, 500, False),
    ('D050', 'Functional', 'Cruise control malfunction', 3, 45, 400, True),
    ('D051', 'Functional', 'Park assist not working', 3, 40, 380, False),
    ('D052', 'Functional', 'Keyless entry fault', 2, 30, 280, False),
    ('D053', 'Functional', 'Backup camera issue', 3, 35, 350, False),
    ('D054', 'Functional', 'Navigation system error', 2, 40, 320, False),
    ('D055', 'Functional', 'Bluetooth connectivity', 2, 25, 200, False),
    ('D056', 'Functional', 'Lane departure warning fault', 3, 50, 450, True),
]

# Format: (code, category, description, severity, repair_time_min, repair_cost_usd, is_safety_critical)

# ================================================================
# MAIN LOAD PROCESS
# ================================================================

def main():
    print("=" * 60)
    print("LOAD dim_quality_defect - Defect Catalog")
    print("=" * 60)
    
    print(f"\n[1/3] Defect catalog prepared")
    print(f"    Total defects: {len(DEFECT_CATALOG)}")
    
    # Count by category
    categories = {}
    for defect in DEFECT_CATALOG:
        cat = defect[1]
        categories[cat] = categories.get(cat, 0) + 1
    
    print(f"    Categories: {len(categories)}")
    for cat, count in sorted(categories.items()):
        print(f"      - {cat}: {count} defects")
    
    # Connect to PostgreSQL
    print(f"\n[2/3] Connecting to PostgreSQL...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ Connected to database")
    except Exception as e:
        print(f"❌ ERROR: Connection failed: {e}")
        return
    
    # Load data
    print(f"\n[3/3] Loading to dim_quality_defect...")
    
    # Truncate table
    try:
        cursor.execute("TRUNCATE TABLE warehouse.dim_quality_defect RESTART IDENTITY CASCADE;")
        conn.commit()
        print("✅ Truncated existing data")
    except Exception as e:
        print(f"❌ ERROR: Truncate failed: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return
    
    # Insert defects
    insert_sql = """
        INSERT INTO warehouse.dim_quality_defect (
            defect_code,
            defect_category,
            defect_description,
            severity_level,
            avg_repair_time_min,
            avg_repair_cost,
            is_safety_critical
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        execute_batch(cursor, insert_sql, DEFECT_CATALOG, page_size=100)
        conn.commit()
        print(f"✅ Inserted {len(DEFECT_CATALOG)} defects")
    except Exception as e:
        conn.rollback()
        print(f"❌ ERROR: Insert failed: {e}")
        cursor.close()
        conn.close()
        return
    
    # Verify
    print(f"\n[4/4] Verifying...")
    
    cursor.execute("SELECT COUNT(*) FROM warehouse.dim_quality_defect;")
    count = cursor.fetchone()[0]
    print(f"✅ Total rows: {count}")
    
    # Summary by category
    cursor.execute("""
        SELECT 
            defect_category,
            COUNT(*) as defect_count,
            ROUND(AVG(avg_repair_cost), 2) as avg_cost,
            COUNT(*) FILTER (WHERE is_safety_critical = TRUE) as safety_critical_count
        FROM warehouse.dim_quality_defect
        GROUP BY defect_category
        ORDER BY avg_cost DESC;
    """)
    
    print(f"\n📊 Summary by Category:")
    print(f"{'Category':<15} {'Count':<8} {'Avg Cost':<12} {'Safety Critical':<15}")
    print("-" * 60)
    
    for row in cursor.fetchall():
        print(f"{row[0]:<15} {row[1]:<8} ${row[2]:<11,.2f} {row[3]:<15}")
    
    # Safety-critical defects
    cursor.execute("""
        SELECT 
            defect_code,
            defect_category,
            defect_description,
            severity_level,
            avg_repair_cost
        FROM warehouse.dim_quality_defect
        WHERE is_safety_critical = TRUE
        ORDER BY severity_level DESC, avg_repair_cost DESC
        LIMIT 10;
    """)
    
    print(f"\n⚠️  Top 10 Safety-Critical Defects:")
    print(f"{'Code':<8} {'Category':<15} {'Description':<40} {'Severity':<10} {'Cost':<10}")
    print("-" * 90)
    
    for row in cursor.fetchall():
        print(f"{row[0]:<8} {row[1]:<15} {row[2]:<40} {row[3]:<10} ${row[4]:<9,.0f}")
    
    # Cost analysis
    cursor.execute("""
        SELECT 
            SUM(avg_repair_cost) as total_potential_cost,
            AVG(avg_repair_cost) as avg_repair_cost,
            MAX(avg_repair_cost) as max_repair_cost
        FROM warehouse.dim_quality_defect;
    """)
    
    cost_stats = cursor.fetchone()
    print(f"\n💰 Cost Analysis:")
    print(f"    Total potential cost (all defects): ${cost_stats[0]:,.2f}")
    print(f"    Average repair cost: ${cost_stats[1]:,.2f}")
    print(f"    Most expensive defect: ${cost_stats[2]:,.2f}")
    
    # Sample defects
    cursor.execute("""
        SELECT 
            defect_code,
            defect_category,
            defect_description,
            severity_level
        FROM warehouse.dim_quality_defect
        ORDER BY RANDOM()
        LIMIT 5;
    """)
    
    print(f"\n🎲 Sample Defects:")
    for row in cursor.fetchall():
        print(f"    [{row[0]}] {row[1]}: {row[2]} (Severity: {row[3]})")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 60)
    print("✅ ETL COMPLETE!")
    print("=" * 60)
    print("\n💡 Business Value:")
    print("   - Quality defect taxonomy standardized")
    print("   - Cost per defect type tracked")
    print("   - Safety-critical defects flagged")
    print("   - Ready for root cause analysis")


if __name__ == "__main__":
    main()
