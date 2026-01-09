# ETL Pipeline Design

**Project:** Stellantis Manufacturing Analytics  
**Version:** Week 3  
**Author:** Vanel  
**Date:** January 2025

---

## 🎯 ETL Overview

**Purpose:** Load production analytics data from CSV files into PostgreSQL data warehouse

**Pattern:** ELT (Extract-Load-Transform)
- Extract: Read CSV files
- Load: Insert raw into staging schema
- Transform: Clean, join, calculate → warehouse schema

**Tools:** Python 3.12 + Pandas + psycopg2

---

## 📊 Data Flow Architecture
```
CSV Files (3 sources)
    ↓
[Python ETL: Extract]
    ↓
Staging Schema (3 tables) ← RAW DATA
    ↓
[Python ETL: Transform]
    ↓
Warehouse Schema (Fact + Dims) ← CLEAN DATA
    ↓
[Business Analytics]
```

---

## 🔄 ETL Stages

### **Stage 1: Extract (CSV → Staging)**

**Scripts:**
- `scripts/etl/load_staging_production.py`
- `scripts/etl/load_staging_maintenance.py`
- `scripts/etl/load_staging_quality.py`

**Process:**
1. Read CSV file (pandas)
2. Minimal validation (file exists, columns present)
3. Truncate staging table
4. Bulk insert to staging (execute_batch)
5. Log row count

**No transformations!** Just copy CSV → PostgreSQL

**Why staging?**
- Preserve original data (audit trail)
- Enable re-runs without re-reading CSV
- Isolate raw data from clean warehouse

---

### **Stage 2: Transform (Staging → Warehouse)**

**Script:** `scripts/etl/transform_load_fact.py`

**Process:**

**2.1 Lookup Dimension Keys**
```python
# Staging has strings
line_name = "Line_1"
shift_name = "Morning"
vehicle_make = "Jeep"
vehicle_model = "Wrangler"
vehicle_year = 2024

# Transform to warehouse foreign keys
line_key = get_line_key("Line_1")           # → 1
shift_key = get_shift_key("Morning")        # → 1
vehicle_key = get_vehicle_key("Jeep", "Wrangler", 2024)  # → 5432
date_key = production_date                  # → '2024-01-15' (already DATE)
```

**2.2 Calculate Derived Metrics**
```python
# OEE already in CSV, but validate
if oee_percent < 0 or oee_percent > 100:
    raise ValueError("Invalid OEE")

# Future: Calculate quality rate
quality_rate = (units_produced - defects) / units_produced * 100
```

**2.3 Validate Business Rules**
```python
# Rule 1: Defects cannot exceed production
if defects > units_produced:
    raise ValueError("Defects > Production")

# Rule 2: Downtime must be positive
if downtime_minutes < 0:
    raise ValueError("Negative downtime")

# Rule 3: Cycle time must be reasonable
if cycle_time_seconds < 1 or cycle_time_seconds > 1000:
    log_warning("Unusual cycle time")
```

**2.4 Load to Fact Table**
```python
INSERT INTO warehouse.fact_production_metrics (
    date_key, line_key, vehicle_key, shift_key,
    units_produced, units_target, defects,
    downtime_minutes, cycle_time_seconds, oee_percent
) VALUES (...);
```

---

### **Stage 3: Orchestration**

**Script:** `scripts/etl/run_etl_pipeline.py`

**Process:**
```python
def run_full_etl():
    print("=== ETL Pipeline Starting ===")
    
    # Step 1: Truncate staging (clean slate)
    truncate_staging_tables()
    
    # Step 2: Load staging from CSVs
    load_staging_production()
    load_staging_maintenance()
    load_staging_quality()
    
    # Step 3: Validate staging data
    row_count = validate_staging_loads()
    if row_count == 0:
        raise Exception("Staging load failed")
    
    # Step 4: Truncate warehouse fact table
    truncate_fact_table()
    
    # Step 5: Transform and load warehouse
    transform_load_fact()
    
    # Step 6: Validate warehouse data
    validate_fact_table()
    
    print("=== ETL Pipeline Complete ===")
```

**Idempotent:** Can run multiple times safely (truncate + insert)

---

## 📋 Data Mappings

### **Production Metrics CSV → Staging**

| CSV Column | Staging Column | Type | Notes |
|------------|----------------|------|-------|
| date | production_date | DATE | Parse "YYYY-MM-DD" |
| line | line_name | VARCHAR(50) | "Line_1", "Line_2"... |
| shift | shift_name | VARCHAR(20) | "Morning", "Afternoon", "Night" |
| make | vehicle_make | VARCHAR(50) | "Jeep", "Ram"... |
| model | vehicle_model | VARCHAR(100) | "Wrangler", "1500"... |
| year | vehicle_year | INTEGER | 2020-2025 |
| units_produced | units_produced | INTEGER | Actual production |
| units_target | units_target | INTEGER | Planned production |
| defects | defects | INTEGER | Defective units |
| downtime_minutes | downtime_minutes | DECIMAL | Unplanned downtime |
| cycle_time | cycle_time_seconds | DECIMAL | Avg time per unit |
| oee | oee_percent | DECIMAL | Pre-calculated OEE |

---

### **Staging → Fact Table (Transformations)**

| Staging Column | Transform | Fact Column | Notes |
|----------------|-----------|-------------|-------|
| production_date | None | date_key (FK) | DATE type |
| line_name | Lookup dim_production_line | line_key (FK) | String → Integer |
| shift_name | Lookup dim_shift | shift_key (FK) | String → Integer |
| vehicle_make + model + year | Lookup dim_vehicle | vehicle_key (FK) | Composite lookup |
| units_produced | Validate >= 0 | units_produced | Copy with validation |
| units_target | Validate >= 0 | units_target | Copy with validation |
| defects | Validate <= units_produced | defects | Business rule check |
| downtime_minutes | Validate >= 0 | downtime_minutes | Copy with validation |
| cycle_time_seconds | Validate > 0 | cycle_time_seconds | Copy with validation |
| oee_percent | Validate 0-100 | oee_percent | Copy with validation |

---

## ⚠️ Error Handling Strategy

### **Missing Dimension Records**

**Problem:** Staging references vehicle that doesn't exist in dim_vehicle

**Solutions:**

**Option A: Fail Fast (Week 3 approach)**
```python
vehicle_key = get_vehicle_key(make, model, year)
if vehicle_key is None:
    raise ValueError(f"Vehicle not found: {make} {model} {year}")
# → ETL stops, admin investigates
```

**Option B: Unknown Placeholder (Production approach)**
```python
vehicle_key = get_vehicle_key(make, model, year)
if vehicle_key is None:
    vehicle_key = get_vehicle_key("Unknown", "Unknown", 9999)
    log_warning(f"Unknown vehicle: {make} {model} {year}")
# → ETL continues, bad records flagged
```

**Week 3 Decision:** Use Option A (fail fast) because:
- We control the data (synthetic CSV we generated)
- Want to catch data issues immediately
- Production would use Option B

---

### **Data Quality Issues**

**Validation Failures:**
```python
try:
    validate_business_rules(record)
    insert_to_warehouse(record)
except ValidationError as e:
    log_error(f"Row {row_num}: {e}")
    rejected_records.append(record)

# After ETL:
if len(rejected_records) > 0:
    print(f"⚠️ {len(rejected_records)} records rejected")
    save_rejected_records_to_csv()
```

---

## 📊 Expected Data Volumes

| Table | Source Rows | Expected Staging | Expected Warehouse |
|-------|-------------|------------------|-------------------|
| **stg_production_metrics** | 1,350 (CSV) | 1,350 | → |
| **stg_maintenance** | 10,000 (CSV) | 10,000 | → (used for equipment dim) |
| **stg_quality** | 1,567 (CSV) | 1,567 | → (used for ML, Week 5) |
| **fact_production_metrics** | ← | ← | 1,350 |

**Calculation:** 90 days × 5 lines × 3 shifts = 1,350 production records

---

## ⏱️ Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| **CSV → Staging** | <10 seconds | 1,350 rows, minimal processing |
| **Staging → Warehouse** | <30 seconds | Lookups + validation |
| **Full ETL pipeline** | <1 minute | End-to-end |
| **Query performance** | <1 second | Simple aggregations |

**Why fast?**
- Small dataset (1,350 rows)
- Indexes on FK columns
- Batch inserts (execute_batch)

---

## 🔐 Data Quality Checks

### **Staging Validation**
```sql
-- Check 1: Row count matches CSV
SELECT COUNT(*) FROM staging.stg_production_metrics;
-- Expected: 1,350

-- Check 2: No null dates
SELECT COUNT(*) FROM staging.stg_production_metrics
WHERE production_date IS NULL;
-- Expected: 0

-- Check 3: Date range reasonable
SELECT MIN(production_date), MAX(production_date)
FROM staging.stg_production_metrics;
-- Expected: ~90 day range
```

### **Warehouse Validation**
```sql
-- Check 1: No orphan foreign keys
SELECT COUNT(*) 
FROM warehouse.fact_production_metrics f
LEFT JOIN warehouse.dim_date d ON f.date_key = d.date
WHERE d.date IS NULL;
-- Expected: 0

-- Check 2: OEE in valid range
SELECT COUNT(*) 
FROM warehouse.fact_production_metrics
WHERE oee_percent NOT BETWEEN 0 AND 100;
-- Expected: 0

-- Check 3: No duplicates (grain = date + line + shift)
SELECT date_key, line_key, shift_key, COUNT(*)
FROM warehouse.fact_production_metrics
GROUP BY date_key, line_key, shift_key
HAVING COUNT(*) > 1;
-- Expected: 0 rows
```

---

## 📅 ETL Schedule (Future - Week 5 with Airflow)
```
Daily ETL (automated):
- Time: 06:00 AM (before business hours)
- Trigger: Airflow DAG
- Steps:
  1. Check for new CSV files
  2. Run full ETL pipeline
  3. Validate data quality
  4. Send success/failure email
  5. Refresh Power BI datasets

Manual ETL (Week 3):
- Run: python scripts/etl/run_etl_pipeline.py
- Frequency: On-demand
```

---

## ✅ Success Criteria

| Criteria | Target | Validation |
|----------|--------|------------|
| **Staging loaded** | 1,350 rows | `SELECT COUNT(*) FROM staging.stg_production_metrics` |
| **Warehouse loaded** | 1,350 rows | `SELECT COUNT(*) FROM warehouse.fact_production_metrics` |
| **No orphan FKs** | 0 | FK validation query |
| **Data quality** | >98% | Validation script |
| **Performance** | <1 min | ETL execution time |
| **Idempotent** | Yes | Run ETL twice, same result |

---

**Design Status:** ✅ Complete  
**Next Step:** Implement ETL scripts (Day 16-17)

---

*This design serves as the blueprint for ETL implementation and future enhancements.*
