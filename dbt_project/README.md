# 🏭 Stellantis Manufacturing Analytics - dbt Project

**Week 4 Deliverable:** Advanced data transformations with dbt

---

## 📋 Project Overview

This dbt project transforms raw manufacturing data into business-ready analytics for:
- Production line optimization
- Predictive maintenance
- Quality control
- Cost analysis
- Executive dashboards

---

## 🏗️ Architecture

### Data Layers

1. **Staging** (3 models): Clean raw data
2. **Intermediate** (4 models): Business logic & feature engineering
3. **Marts** (6 models): Analytics-ready tables
4. **Dimensions** (1 model): SCD Type 2 historical tracking

### Technology Stack

- **Database:** PostgreSQL
- **Transformation:** dbt 1.8.0
- **Testing:** dbt_utils + dbt_expectations
- **Orchestration:** Bash scripts

---

## 🚀 Quick Start

### Prerequisites
```bash
# Python 3.8+
python --version

# PostgreSQL running
psql --version

# Virtual environment active
source venv/bin/activate
```

### Installation
```bash
# Install dbt packages
dbt deps

# Test connection
dbt debug
```

### Run Pipeline
```bash
# Full refresh (all layers + tests)
./run_full_refresh.sh

# Quick run (models only)
./run_quick.sh

# Run specific layer
./run_layer.sh marts
```

---

## 📊 Key Features

### Business Intelligence
- ✅ Executive KPIs (daily factory performance)
- ✅ Production line rankings & comparisons
- ✅ Shift analysis & optimization
- ✅ Predictive maintenance alerts
- ✅ Quality trend analysis
- ✅ Cost breakdown & ROI opportunities

### Data Quality
- ✅ 42 automated tests (100% passing)
- ✅ Business rule validation
- ✅ Data integrity checks
- ✅ Anomaly detection
- ✅ Quality score: 97.8%

### Advanced Features
- ✅ SCD Type 2 (historical tracking)
- ✅ Custom macros (reusable logic)
- ✅ Pre/post hooks automation
- ✅ Full orchestration scripts
- ✅ Row count reporting
- ✅ Auto-generated documentation

---

## 📂 Project Structure
```
dbt_project/
├── models/
│   ├── staging/           # Raw data cleaning
│   ├── intermediate/      # Business logic
│   ├── marts/             # Analytics tables
│   └── dimensions/        # SCD Type 2
├── macros/
│   ├── classify_oee.sql
│   ├── calculate_defect_rate.sql
│   └── operations.sql
├── tests/
│   └── business_rules/    # Custom tests
├── analyses/
│   └── data_quality_summary.sql
├── run_full_refresh.sh    # Full pipeline
├── run_quick.sh           # Fast run
├── run_tests.sh           # Tests only
└── run_layer.sh           # Layer-specific
```

---

## 📈 Metrics

### Build Performance
- **Full Refresh:** ~20 seconds
- **Quick Run:** ~5 seconds
- **Tests:** ~3 seconds

### Data Volume
- **Staging:** 12,527 rows
- **Intermediate:** 12,527 rows (views)
- **Marts:** 10,136 rows (tables)
- **Dimensions:** 5 rows

### Quality Metrics
- **Tests:** 42 total (100% passing)
- **Data Quality:** 97.8%
- **Coverage:** All models tested

---

## 🎯 Use Cases

### For Executives
```sql
-- Daily factory performance
SELECT * FROM dbt_dev_marts.mart_executive_kpis 
WHERE date = CURRENT_DATE;
```

### For Operations Managers
```sql
-- Which lines need attention?
SELECT production_line, avg_oee, priority_level
FROM dbt_dev_marts.mart_line_performance
ORDER BY overall_performance_score;
```

### For Maintenance Team
```sql
-- Machines at risk
SELECT product_id, risk_category, action_recommendation
FROM dbt_dev_marts.mart_maintenance_overview
WHERE risk_category IN ('CRITICAL', 'HIGH')
ORDER BY days_until_maintenance;
```

---

## 🧪 Testing
```bash
# Run all tests
dbt test

# Run specific layer tests
dbt test --select marts.*

# Run business rule tests
dbt test --select test_type:data

# Get data quality report
dbt run-operation get_row_counts
```

---

## 📚 Documentation

### Generate Docs
```bash
dbt docs generate
dbt docs serve
```

Then open: http://localhost:8080

### Features
- ✅ Data lineage graphs
- ✅ Column-level documentation
- ✅ Test results
- ✅ Model dependencies
- ✅ SQL compilation

---

## 🛠️ Maintenance

### Operations
```bash
# Get row counts
dbt run-operation get_row_counts

# Truncate staging
dbt run-operation truncate_staging_tables

# Refresh views
dbt run-operation refresh_all_views
```

---

## 👥 Team

**Project:** Stellantis Manufacturing Analytics  
**Week:** 4 (dbt Transformations)  
**Duration:** 7 days  
**Status:** ✅ Complete

---

## 📝 License

Internal Stellantis project - Proprietary

---

## 🎉 Achievements

- ✅ 14 models built & tested
- ✅ 42 automated tests (100% passing)
- ✅ 97.8% data quality score
- ✅ Full automation (<20 sec refresh)
- ✅ Production-ready deliverable
