# 🏗️ Project Architecture

**Project:** Stellantis Manufacturing Performance Analytics  
**Version:** Week 1 Complete  
**Last Updated:** January 2025

---

## 📊 High-Level Architecture
```
┌─────────────────────────────────────────────────────────────┐
│              STELLANTIS MANUFACTURING ANALYTICS              │
│                   End-to-End Data Pipeline                   │
└─────────────────────────────────────────────────────────────┘

                    ┌──────────────────┐
                    │   RAW DATA       │
                    │   SOURCES        │
                    └────────┬─────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
   ┌────▼────┐         ┌─────▼─────┐      ┌──────▼──────┐
   │ Kaggle  │         │ Synthetic │      │   Manual    │
   │ Datasets│         │   Data    │      │   Upload    │
   └────┬────┘         └─────┬─────┘      └──────┬──────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │
                    ┌────────▼─────────┐
                    │  DATA LANDING    │
                    │  Zone (CSV)      │
                    │  /data/raw/      │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  PYTHON ETL      │
                    │  Cleaning        │
                    │  Validation      │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  POSTGRESQL      │
                    │  Data Warehouse  │
                    │  (Star Schema)   │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
        ┌─────▼─────┐  ┌─────▼─────┐  ┌────▼────┐
        │  Airflow  │  │    dbt    │  │   ML    │
        │Orchestrate│  │ Transform │  │ Models  │
        └─────┬─────┘  └─────┬─────┘  └────┬────┘
              │              │              │
              └──────────────┼──────────────┘
                             │
                    ┌────────▼─────────┐
                    │   POWER BI       │
                    │  Dashboards      │
                    └──────────────────┘
```

---

## 🔄 Data Flow - Detailed

### Layer 1: Data Sources 📥
```
SOURCES:
├─ Kaggle API
│  ├─ Predictive Maintenance (10K rows)
│  ├─ Quality Control (1.5K rows)
│  └─ Vehicle Specs (11.9K rows)
│
├─ Synthetic Generation
│  └─ Production Metrics (1.3K rows)
│
└─ Future: Real-time Sensors (TBD)

FORMAT: CSV files
LOCATION: /data/raw/
REFRESH: Manual (Week 1), Automated (Week 3+)
```

---

### Layer 2: Data Landing Zone 📁
```
STRUCTURE:
/data/raw/
├── maintenance/
│   └── ai4i2020.csv
├── quality/
│   └── uci-secom.csv
├── vehicles/
│   └── data.csv
└── production/
    └── production_metrics.csv

PURPOSE:
- Raw data storage
- Version control friendly
- Immutable (never modify originals)
- Backup point

VALIDATION:
- Schema checks
- File integrity
- Size validation
```

---

### Layer 3: Python ETL Pipeline 🐍
```
COMPONENTS:

1. Data Ingestion
   scripts/etl/ingest_*.py
   - Read CSV files
   - Initial validation
   - Load to PostgreSQL staging

2. Data Cleaning
   scripts/etl/clean_*.py
   - Handle missing values
   - Remove duplicates
   - Outlier treatment
   - Data type conversions

3. Data Quality
   scripts/quality/assess_data_quality.py
   - 6 dimensions assessment
   - Automated scoring
   - Quality reports

4. Feature Engineering
   scripts/etl/feature_engineering.py
   - OEE calculations
   - Derived metrics
   - ML feature prep

TECH STACK:
- Python 3.12
- Pandas (data manipulation)
- NumPy (numerical operations)
- SQLAlchemy (database ORM)
```

---

### Layer 4: PostgreSQL Data Warehouse 🗄️
```
SCHEMA: Star Schema (Week 2 design)

FACT TABLE:
fact_production_metrics
├── metric_id (PK)
├── date_key (FK)
├── line_key (FK)
├── equipment_key (FK)
├── vehicle_key (FK)
├── shift_key (FK)
├── [measures: production, downtime, defects, OEE]
└── created_at

DIMENSION TABLES:
1. dim_date (time dimension)
2. dim_production_line (5 lines)
3. dim_equipment (from maintenance dataset)
4. dim_vehicle (from vehicles dataset)
5. dim_shift (3 shifts)
6. dim_quality_defect (from quality dataset)

SPECIAL TABLES:
- ml_maintenance_predictions (ML output)
- quality_metrics_log (DQ tracking)
- staging schema (raw data)

FEATURES:
- Indexes for performance
- Foreign key constraints
- Check constraints (data validation)
- Audit columns (created_at, updated_at)
```

---

### Layer 5: Orchestration & Transformation ⚙️
```
APACHE AIRFLOW:
DAG: manufacturing_analytics_pipeline

Tasks:
├─ 1. check_new_data
├─ 2. ingest_data (parallel)
├─ 3. data_quality_checks
├─ 4. clean_transform
├─ 5. ml_predictions
├─ 6. load_warehouse
└─ 7. dbt_transformations

Schedule: Daily @ 6:00 AM
Retry: 3 attempts
Notifications: Email/Slack

---

DBT (Data Build Tool):
Models:
├── staging/
│   ├── stg_products
│   ├── stg_vendors
│   └── stg_sales
│
└── marts/
    ├── mart_executive_kpis
    ├── mart_revenue_by_vendor
    ├── mart_sales_by_category
    └── mart_top_products

Tests: 28 automated tests
Documentation: Lineage graphs
```

---

### Layer 6: ML & Analytics 🤖
```
MACHINE LEARNING:

Model: Random Forest Classifier
Purpose: Predictive Maintenance
Input: Temperature, RPM, Torque, Tool Wear
Output: Failure Probability (0-100%)

Training:
- Dataset: 10K historical records
- Features: 8 engineered features
- Target: Machine failure (binary)
- Validation: 80/20 split

Deployment:
- Batch predictions daily
- Stored in ml_maintenance_predictions
- Integrated in Airflow DAG
```

---

### Layer 7: Visualization 📊
```
POWER BI:

Dashboards:
1. Executive KPIs
   - Overall OEE
   - Production volumes
   - Quality metrics
   - Financial KPIs

2. Predictive Maintenance
   - Equipment health scores
   - Failure predictions
   - Maintenance schedule

3. Quality Control
   - Defect trends
   - Root cause analysis
   - Cost of quality

4. Simulation Scenarios
   - What-if analysis
   - Capacity planning
   - ROI calculations

Refresh: Daily @ 7:00 AM (after Airflow)
Access: Role-based (CEO, Managers, Engineers)
```

---

## 🔐 Security & Governance
```
DATA SECURITY:
- Database: Role-based access control
- API Keys: Environment variables (.env)
- Credentials: Never in Git (.gitignore)
- Backups: Daily automated

DATA GOVERNANCE:
- Data lineage: dbt docs
- Change tracking: Git history
- Quality monitoring: Automated scores
- Audit logs: Database triggers

COMPLIANCE:
- GDPR: No personal data
- Industry standards: SAE, AIAG
```

---

## 📊 Current Implementation Status

### Week 1: ✅ COMPLETE
```
✅ Data sources identified
✅ Data landing zone established
✅ Python scripts (generation, quality)
✅ Documentation complete
```

### Week 2: ⏳ IN PROGRESS
```
⏳ PostgreSQL warehouse design
⏳ Star schema DDL
⏳ Database setup
```

### Week 3-4: ⏳ UPCOMING
```
⏳ ETL pipeline development
⏳ Data cleaning scripts
⏳ Warehouse population
```

### Week 5+: ⏳ PLANNED
```
⏳ ML model training
⏳ Airflow DAG creation
⏳ dbt transformations
⏳ Power BI dashboards
```

---

## 🎯 Architecture Decisions

### Why Star Schema?
- ✅ Optimized for analytics queries
- ✅ Denormalized for speed
- ✅ Industry standard for data warehouses
- ✅ Easy to understand for business users

### Why Python for ETL?
- ✅ Rich ecosystem (Pandas, NumPy, scikit-learn)
- ✅ Industry standard for data engineering
- ✅ Easy integration with ML libraries
- ✅ Excellent for data manipulation

### Why Airflow?
- ✅ Industry standard for orchestration
- ✅ Scalable and reliable
- ✅ Rich UI for monitoring
- ✅ Extensive integrations

### Why dbt?
- ✅ SQL-based (accessible to analysts)
- ✅ Built-in testing framework
- ✅ Auto-generated documentation
- ✅ Modern data transformation best practices

### Why Power BI?
- ✅ Used by Stellantis (Microsoft stack)
- ✅ Rich visualization capabilities
- ✅ Enterprise features (row-level security)
- ✅ Integration with Azure

---

## 📈 Scalability Considerations

**Current:** Single-machine, batch processing  
**Future:** Distributed processing with Spark if needed

**Data Volume Scaling:**
- Current: 25K rows → PostgreSQL OK
- 1M rows → Still OK
- 100M+ rows → Consider Spark + Parquet

**Compute Scaling:**
- Current: Local Python
- Future: Docker containers
- Production: Kubernetes cluster

---

## 🔄 CI/CD Pipeline (Future)
```
GitHub Actions:
├─ On Push:
│  ├─ Lint Python code (flake8)
│  ├─ Run unit tests (pytest)
│  └─ Check SQL syntax
│
└─ On Merge to Main:
   ├─ Deploy to staging
   ├─ Run integration tests
   └─ Deploy to production (manual approval)
```

---

**Last Updated:** January 2025  
**Status:** Living document - updated weekly
