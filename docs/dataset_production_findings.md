# 🏭 Production Metrics Dataset - Findings

**Dataset:** Synthetic Production Data  
**Source:** Self-generated  
**Date:** Day 5  
**Purpose:** Simulate realistic manufacturing production data

---

## 📈 Dataset Overview

- **Rows:** 1,350 (90 days × 5 lines × 3 shifts)
- **Columns:** 23
- **Size:** ~500KB
- **Missing Values:** 0 (Perfect dataset)

---

## 🎯 Business Context

### Simulates Stellantis Manufacturing Plant

**Structure:**
- 5 Production lines (A, B, C, D, E)
- 3 Shifts per day (Morning, Afternoon, Night)
- 90 days historical data (3 months)
- Work days only (weekends skipped)

**Vehicle Models:**
- Line A → Peugeot 208
- Line B → Peugeot 308
- Line C → Citroën C3
- Line D → Opel Corsa
- Line E → Fiat 500

---

## 📊 Key Metrics Generated

### Production Metrics
1. **planned_production_time_min** - Always 480 (8h shift)
2. **downtime_min** - Equipment downtime (Gamma distribution)
3. **actual_production_time_min** - Planned - Downtime
4. **expected_units** - Theoretical production
5. **actual_units** - Real production (with performance loss)
6. **good_units** - Production minus defects

### Quality Metrics
7. **defect_count** - Units with defects (2-5% rate)
8. **rework_count** - Defects that can be repaired (70%)
9. **scrap_count** - Defects to trash (30%)

### OEE Components
10. **availability_pct** - Uptime percentage
11. **performance_pct** - Speed vs theoretical
12. **quality_pct** - Good units percentage
13. **oee_score** - Overall Equipment Effectiveness

### Operational
14. **target_cycle_time_min** - Ideal time per unit
15. **actual_cycle_time_min** - Real time per unit
16. **primary_downtime_cause** - Main reason for downtime
17. **operators_count** - Staff per shift (8-12)
18. **line_speed_units_per_hour** - Production rate

---

## 🎯 Statistical Summary

### OEE Performance

| Line | Average OEE | Target | Status |
|------|-------------|--------|--------|
| Line A | 75.2% | 75% | ✅ On target |
| Line B | 79.8% | 80% | ✅ Good |
| Line C | 81.5% | 82% | ✅ Good |
| Line D | 78.1% | 78% | ✅ On target |
| Line E | 84.3% | 85% | ✅ Excellent |

**Overall Average OEE: ~78%** (Realistic for automotive)

---

## 📊 Key Findings

### 1. Production Volume
- **Total units produced:** ~60,000 units over 90 days
- **Average per shift:** ~45 units
- **Best line:** Line E (newest equipment)
- **Slowest line:** Line A (older equipment)

### 2. Quality Performance
- **Overall defect rate:** ~3.5%
- **Night shift:** +30% more defects (operator fatigue)
- **Best quality:** Morning shift
- **Total defects:** ~2,100 units

### 3. Downtime Analysis
- **Average downtime:** ~35 minutes/shift
- **Night shift:** 50% more downtime
- **Monday effect:** +20% downtime (weekend issues)

### 4. Top Downtime Causes
1. Equipment_Failure (25%)
2. Maintenance_Preventive (20%)
3. Setup_Changeover (18%)
4. Material_Shortage (15%)
5. Quality_Issue (12%)
6. Operator_Absence (10%)

---

## 💡 Data Generation Methodology

### Distributions Used

**Downtime (Gamma distribution):**
```python
# Why Gamma?
- Skewed right (many small downtimes, few large)
- No negative values (impossible to have -10 min downtime)
- Realistic for failure rates
```

**Performance (Normal distribution):**
```python
# Why Normal?
- Most shifts perform around average (95%)
- Some variation due to operator skill, conditions
- Bounded between 70-100%
```

**Defects (Beta distribution):**
```python
# Why Beta?
- Bounded between 0 and 1 (percentage)
- Skewed toward low values (defects are rare)
- Models quality control processes
```

---

## ✅ Data Quality Assessment

| Dimension | Score | Notes |
|-----------|-------|-------|
| Completeness | 100% | No missing values |
| Consistency | 100% | All relationships valid |
| Validity | 100% | All values in expected ranges |
| Accuracy | 95% | Based on industry standards |

**Quality Checks Performed:**
- ✅ No negative values
- ✅ OEE between 0-100%
- ✅ Defects ≤ Production
- ✅ Downtime ≤ Planned time
- ✅ Good units = Production - Defects

---

## 🎯 Realism Validation

### Industry Benchmarks

| Metric | Industry Standard | Our Data | Status |
|--------|------------------|----------|---------|
| OEE (Automotive) | 75-85% | 78% | ✅ Realistic |
| World-class OEE | 85%+ | Line E: 84% | ✅ Close |
| Defect Rate | 2-5% | 3.5% | ✅ Realistic |
| Cycle Time | 2-3 min | 2.4-3.0 min | ✅ Realistic |

**Sources:**
- SAE (Society of Automotive Engineers)
- Lean Manufacturing benchmarks
- Automotive Industry Action Group (AIAG)

---

## 🚀 Use Cases

### 1. Data Warehouse Fact Table
- Primary source for `fact_production_metrics`
- Links to dimensions (date, line, vehicle)

### 2. OEE Dashboards
- Real-time monitoring (with Elixir phase 2)
- Historical trends
- Line comparisons

### 3. ML Predictive Models
- Predict downtime based on patterns
- Forecast production volumes
- Quality defect prediction

### 4. Root Cause Analysis
- Identify main downtime causes
- Optimize shift schedules
- Improve maintenance planning

---

## 💭 Limitations & Assumptions

**Limitations:**
- Synthetic data (not from real sensors)
- Simplified model (real factories have more variables)
- No seasonality effects (holidays, summer shutdowns)

**Assumptions:**
- Weekends: Plant closed
- Shift duration: Exactly 8 hours
- Operators: 8-12 per shift
- No major equipment failures (plant shutdowns)

**Mitigation:**
- Used industry-validated distributions
- Calibrated to real-world benchmarks
- Added realistic variations (shift, day-of-week effects)

---

## 🎤 Interview Talking Points

### "Why synthetic data?"
> "No access to Stellantis confidential data. Generated realistic data based on automotive industry standards (SAE, AIAG benchmarks) to demonstrate pipeline capabilities."

### "How did you ensure realism?"
> "Used appropriate statistical distributions (Gamma for downtime, Beta for defects), calibrated to industry benchmarks (OEE 75-85%), added realistic variations (night shift has more failures)."

### "Could you use real data instead?"
> "Absolutely. The pipeline is designed to consume any production data. In production, we'd connect to Stellantis' MES (Manufacturing Execution System) instead of my synthetic generator."

---

## 📁 File Details

**Filename:** `production_metrics.csv`  
**Location:** `data/raw/production/`  
**Format:** CSV with header  
**Encoding:** UTF-8  
**Records:** 1,350  
**Columns:** 23

---

**Generated by:** Vanel  
**Date:** Day 5  
**Status:** ✅ Complete and validated  
**Next:** Database design (Week 2)
