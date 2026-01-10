# Airflow DAGs - Stellantis Manufacturing Analytics

## Overview

Automated data pipeline orchestration using Apache Airflow 3.1.5

---

## DAGs

### 1. dbt_hourly_staging
**Schedule:** Every hour (`@hourly`)  
**Purpose:** Refresh staging layer with fresh data  
**Duration:** ~30 seconds  
**Models:** 3 staging models

**Tasks:**
- `run_staging_models`: Run staging layer
- `test_staging_models`: Validate staging data
- `log_completion`: Log success

**Use Case:** Real-time dashboards need fresh data

---

### 2. dbt_daily_warehouse
**Schedule:** Daily at 2 AM (`0 2 * * *`)  
**Purpose:** Full warehouse refresh  
**Duration:** ~2 minutes  
**Models:** 14 total (intermediate + marts + dimensions)

**Tasks:**
- `wait_for_staging`: Wait for 1 AM staging run
- `run_intermediate_models`: Transform staging → intermediate
- `run_dimension_models`: Update dimensions (SCD Type 2)
- `run_mart_models`: Build analytics marts
- `run_all_tests`: Execute 42 data quality tests
- `get_row_counts`: Generate metrics report
- `notify_success`: Success notification

**Dependencies:** Requires `dbt_hourly_staging` to complete first

**Use Case:** Daily executive reports and dashboards

---

### 3. dbt_weekly_analytics
**Schedule:** Every Monday at 6 AM (`0 6 * * 1`)  
**Purpose:** Generate weekly analytics reports  
**Duration:** ~1 minute  

**Tasks:**
- `quality_trends_analysis`: Compile quality analysis
- `generate_weekly_report`: Create summary report
- `email_stakeholders`: Send notifications (placeholder)

**Use Case:** Weekly executive briefings

---

## DAG Dependencies
```
dbt_hourly_staging (runs hourly)
         ↓
dbt_daily_warehouse (waits for 1 AM staging, runs at 2 AM)
         ↓
dbt_weekly_analytics (independent, runs Monday 6 AM)
```

---

## Architecture

### Scheduling Strategy

**Hourly (Staging):**
- Lightweight, fast refresh
- Only staging models (3 models)
- Provides real-time data

**Daily (Warehouse):**
- Complete transformation pipeline
- All 14 models + 42 tests
- Production-ready analytics

**Weekly (Reports):**
- Aggregated insights
- Executive summaries
- Stakeholder communications

---

## Configuration

**DBT Project:** `/home/vanel/stellantis-manufacturing-analytics/dbt_project`  
**DBT Binary:** `/home/vanel/stellantis-manufacturing-analytics/venv_stellantis/bin/dbt`  
**Airflow Home:** `~/airflow`  
**DAGs Folder:** `~/stellantis-manufacturing-analytics/airflow_dags`

---

## Monitoring

**Airflow UI:** http://localhost:8080  
**Username:** admin  
**Password:** (set during install)

### Key Metrics:
- DAG run success rate
- Task duration trends
- Data quality test results
- Row count validation

---

## Troubleshooting

### DAG Not Appearing
1. Check DAG file syntax: `python <dag_file>.py`
2. Wait 30 seconds for Airflow to scan
3. Check Airflow logs in terminal

### Task Failing
1. Click task → View Log
2. Check dbt errors in output
3. Test dbt command manually

### Wait Task Timing Out
1. Check if dependent DAG completed
2. Adjust `execution_delta` timing
3. Skip task for manual testing

---

## Usage

### Start Airflow
```bash
cd ~/stellantis-manufacturing-analytics
source venv_stellantis/bin/activate
export AIRFLOW_HOME=~/airflow
airflow standalone
```

### Trigger DAG Manually
```bash
airflow dags trigger dbt_daily_warehouse
```

### Test DAG
```bash
airflow dags test dbt_hourly_staging
```

---

## Maintenance

### Update DAG
1. Edit DAG file
2. Save changes
3. Wait 30 seconds (auto-reload)
4. Trigger test run

### Pause DAG
1. In Airflow UI, toggle DAG off
2. Or: `airflow dags pause <dag_id>`

---

## Future Enhancements

- [ ] Email notifications (SMTP setup)
- [ ] Slack notifications
- [ ] Data quality alerts
- [ ] SLA monitoring
- [ ] Failure callbacks
- [ ] Dynamic DAG generation
