# Stellantis Manufacturing Analytics - dbt Models

## Architecture Overview
```
┌─────────────────────────────────────────────────────────────┐
│                     DATA FLOW ARCHITECTURE                   │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  CSV Files → PostgreSQL Staging → dbt Transformations        │
│                                                               │
│  Layer 1: STAGING (3 models)                                 │
│  ├── stg_production      (Production metrics)                │
│  ├── stg_maintenance     (Machine sensors)                   │
│  └── stg_quality         (Quality sensors)                   │
│                                                               │
│  Layer 2: INTERMEDIATE (4 models)                            │
│  ├── int_production_enriched    (KPIs + classifications)     │
│  ├── int_oee_breakdown          (OEE components)             │
│  ├── int_maintenance_features   (Failure risk)               │
│  └── int_quality_aggregated     (Anomaly detection)          │
│                                                               │
│  Layer 3: MARTS (6 models)                                   │
│  ├── mart_executive_kpis        (Daily CEO dashboard)        │
│  ├── mart_line_performance      (Line rankings)              │
│  ├── mart_shift_analysis        (Shift comparison)           │
│  ├── mart_maintenance_overview  (Predictive maintenance)     │
│  ├── mart_quality_trends        (Quality trends)             │
│  └── mart_cost_analysis         (Cost breakdown)             │
│                                                               │
│  Layer 4: DIMENSIONS (1 model)                               │
│  └── dim_vehicle_scd            (SCD Type 2)                 │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Model Details

### Staging Layer
**Purpose:** Clean and standardize raw data  
**Materialization:** Views (no storage overhead)  
**Refresh:** Real-time (views query source)

### Intermediate Layer
**Purpose:** Business logic and feature engineering  
**Materialization:** Views  
**Use Case:** Reusable transformations for multiple marts

### Marts Layer
**Purpose:** Business-ready analytics tables  
**Materialization:** Tables (optimized for queries)  
**Users:** Executives, analysts, dashboards

### Dimensions Layer
**Purpose:** Slowly Changing Dimensions (historical tracking)  
**Materialization:** Tables (SCD Type 2)

---

## Key Metrics

- **Total Models:** 14
- **Total Tests:** 42
- **Test Coverage:** 100% passing
- **Data Quality Score:** 97.8%
- **Build Time:** ~20 seconds (full refresh)

---

## Usage

### Full Refresh
```bash
./run_full_refresh.sh
```

### Quick Run
```bash
./run_quick.sh
```

### Specific Layer
```bash
./run_layer.sh marts
```

### Documentation
```bash
dbt docs generate
dbt docs serve
```
