# 🔴 PRODUCTION DEPLOYMENT CHECKLIST

## Pre-Deployment

### 1. Test in Development
- [ ] Run `dbt build --target dev`
- [ ] Verify all tests pass (42/42)
- [ ] Check data quality score (>95%)
- [ ] Review model changes

### 2. Code Review
- [ ] Git branch reviewed
- [ ] No hardcoded credentials
- [ ] Documentation updated
- [ ] Tests added for new models

### 3. Backup Production
```bash
# Backup production schemas
pg_dump -U vanel -d stellantis_manufacturing \
  -n dbt_prod_staging \
  -n dbt_prod_intermediate \
  -n dbt_prod_marts \
  -n dbt_prod_dimensions \
  -F c -f backup_prod_$(date +%Y%m%d).dump
```

---

## Deployment Steps

### Step 1: Deploy to Production
```bash
cd ~/stellantis-manufacturing-analytics/dbt_project

# Deploy with production target
dbt build --target prod
```

### Step 2: Validate Deployment
```bash
# Run tests
dbt test --target prod

# Check row counts
dbt run-operation get_row_counts --target prod
```

### Step 3: Compare Environments
```bash
# Trigger comparison DAG in Airflow
airflow dags trigger dbt_env_comparison
```

### Step 4: Monitor
- Check Airflow UI for success
- Review logs for errors
- Verify dashboards updated

---

## Post-Deployment

### Verification
- [ ] All models refreshed
- [ ] All tests passing
- [ ] Row counts match expectations
- [ ] Dashboards showing new data
- [ ] No errors in logs

### Communication
- [ ] Notify stakeholders of deployment
- [ ] Document any breaking changes
- [ ] Update runbook if needed

---

## Rollback Procedure

If deployment fails:

### Option 1: Quick Rollback
```bash
# Restore from backup
pg_restore -U vanel -d stellantis_manufacturing \
  backup_prod_YYYYMMDD.dump
```

### Option 2: Revert Code
```bash
# Git revert
git revert <commit_hash>

# Redeploy previous version
dbt build --target prod
```

---

## Emergency Contacts

**Data Team Lead:** data-team@stellantis.local  
**On-Call:** +33 XXX XXX XXX  
**Airflow:** http://localhost:8080

---

## Production Environment Details

**Target:** `prod`  
**Schemas:** `dbt_prod_*`  
**Schedule:** Daily at 3 AM  
**DAG:** `dbt_prod_daily_refresh`  
**Tests:** 42 automated  
**Retries:** 3 attempts  

---

## Best Practices

✅ Always test in dev first  
✅ Deploy during low-usage hours (3 AM)  
✅ Monitor for 24 hours post-deployment  
✅ Keep backups for 7 days  
✅ Document all changes  
✅ Never deploy on Fridays! 😅
