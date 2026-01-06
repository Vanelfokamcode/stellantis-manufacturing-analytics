# 📊 Predictive Maintenance Dataset - Findings

**Dataset:** AI4I 2020 Predictive Maintenance Dataset  
**Source:** Kaggle - stephanmatzka  
**Date Explored:** Day 3

---

## 📈 Dataset Overview

- **Rows:** 10,000
- **Columns:** 14
- **Size:** ~500KB
- **Missing Values:** 0 (Complete dataset!)

---

## 🔧 Features

### Identifiers
- `UDI` - Unique Device Identifier (1-10000)
- `Product ID` - Product code (L/M/H + serial)

### Product Type
- `Type` - Quality variant (L: Low, M: Medium, H: High)

### Sensor Measurements
1. **Air temperature [K]** - Ambient temperature (296-304K)
2. **Process temperature [K]** - Machine temperature (305-314K)
3. **Rotational speed [rpm]** - Machine speed (1168-2886 RPM)
4. **Torque [Nm]** - Machine torque (3.8-76.6 Nm)
5. **Tool wear [min]** - Cumulative wear (0-253 min)

### Target Variable
- **Machine failure** - Binary (0: No failure, 1: Failure)
  - Failure rate: **3.39%** (339 failures / 10,000)

### Failure Types
- **TWF** - Tool Wear Failure (45 cases)
- **HDF** - Heat Dissipation Failure (115 cases)
- **PWF** - Power Failure (95 cases)
- **OSF** - Overstrain Failure (98 cases)
- **RNF** - Random Failures (18 cases)

---

## 🎯 Key Insights

### 1. Imbalanced Dataset
- Only 3.39% failures → Need to handle class imbalance in ML
- Consider SMOTE or class weights

### 2. Temperature Correlation
- Process temperature slightly higher in failure cases
- Could be a strong predictor

### 3. Quality Variants
- L (Low): 60%
- M (Medium): 30%
- H (High): 10%
- Distribution realistic for manufacturing

### 4. No Missing Data
- ✅ Clean dataset, ready for ML
- Minimal preprocessing needed

---

## 💡 ML Recommendations

### Suitable Models
- Random Forest (handles imbalance well)
- XGBoost (excellent for tabular data)
- Logistic Regression (baseline)

### Feature Engineering Ideas
- Temperature difference (Process - Air)
- Power (Torque × Rotational speed)
- Interaction terms
- Rolling averages (if time-series)

### Evaluation Metrics
- **Don't use accuracy!** (imbalanced)
- Use: Precision, Recall, F1-Score, AUC-ROC
- Focus on recall (catch failures!)

---

## ✅ Data Quality Assessment

| Dimension | Score | Notes |
|-----------|-------|-------|
| Completeness | 100% | No missing values |
| Validity | 100% | All values in expected ranges |
| Consistency | 100% | No duplicates, clean format |
| Timeliness | N/A | Synthetic dataset |

**Overall Quality: 10/10** ⭐

---

## 🚀 Next Steps

1. Feature engineering (Week 5)
2. Train ML model (Week 5-6)
3. Integrate predictions in pipeline (Week 6)

---

**Explored by:** Vanel  
**Date:** Day 3  
**Status:** ✅ Complete
