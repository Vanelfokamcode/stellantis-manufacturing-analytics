# Stellantis Manufacturing Dashboards

## Overview
Power BI dashboards for real-time manufacturing analytics.

## Dashboards

### 1. Executive KPIs
- **Purpose:** High-level daily overview for executives
- **Refresh:** Manual (click Refresh button)
- **Key Metrics:** Production, OEE, Health Status, Costs

### 2. Operations Dashboard  
- **Purpose:** Detailed operations monitoring for plant managers
- **Refresh:** Manual
- **Key Metrics:** Shift performance, Line production, Trends

## How to Use

### Opening the Dashboard:
1. Open `Stellantis_Manufacturing_Dashboards_v1.pbix` in Power BI Desktop
2. Click **Home → Refresh** to get latest data
3. Navigate between pages using bottom tabs

### Filtering Data:
- Use date slicers at top to filter time range
- Click on any chart to cross-filter other visuals
- Right-click → Clear filters to reset

### Refreshing Data:
- **Home ribbon → Refresh button**
- Pulls latest data from PostgreSQL
- Takes ~10-30 seconds

## Connection Details
- **Server:** 172.23.154.165
- **Database:** stellantis_manufacturing
- **Schema:** dbt_prod_marts
- **Tables:** 6 mart tables

## Troubleshooting

### "Cannot connect to database"
- Check PostgreSQL is running: `sudo service postgresql status`
- Check network: `sudo netstat -tlnp | grep 5432`
- Should show: `0.0.0.0:5432`

### "Blank visuals"
- Check date filters (might be filtering out all data)
- Click "Clear filters" on visual
- Refresh data source

## Future Enhancements
- [ ] Setup auto-refresh (requires Power BI Pro)
- [ ] Publish to Power BI Service
- [ ] Add Quality Control dashboard
- [ ] Mobile layout
- [ ] Scheduled refresh

---
Created: Day 33 - Week 5
Author: Vanel
Status: Production Ready ✅
```

**Save:** `Ctrl+O`, `Enter`, `Ctrl+X`

---

## **📊 DAY 33 - COMPLETE STATS:**
```
⏱️ Time Spent: ~3 hours
📊 Dashboards Created: 2
📈 Visuals Built: 12
🎨 Chart Types: 8 (Card, Line, Bar, Column, Area, Donut, Gauge, Matrix)
🔌 Data Source: PostgreSQL (live connection)
📁 File Size: ~5-10 MB
✅ Status: PRODUCTION READY!
```

---

## **🗺️ PROJECT PROGRESS - WEEK 5:**
```
Week 5: Orchestration & Monitoring
✅ Day 29: Airflow Setup & First DAG
✅ Day 30: Multiple Production DAGs
✅ Day 31: Production Environment
✅ Day 32: Monitoring & Alerting
✅ Day 33: Power BI Dashboards ← DONE! 🎉
⏳ Day 34: CI/CD Pipeline (Optional)
⏳ Day 35: Final Documentation & Wrap-up
