# 📊 All Datasets Overview - Day 4

**Date:** Day 4  
**Status:** 3/4 datasets downloaded and explored

---

## 📈 Datasets Summary

| Dataset | Rows | Columns | Size | Missing Values | Status |
|---------|------|---------|------|----------------|--------|
| **Maintenance** | 10,000 | 14 | 522KB | 0 (0%) | ✅ Ready |
| **Quality** | ~1,567 | ~592 | 6MB | High (~40%) | ⚠️ Needs cleaning |
| **Vehicles** | 11,914 | 16 | 1.5MB | Some | ✅ Ready |
| **Production** | TBD | TBD | TBD | TBD | ⏳ Day 5 |

**Total data:** ~23,500 rows

---

## 🔧 Dataset 1: Predictive Maintenance

**Source:** Kaggle - AI4I 2020  
**Purpose:** ML predictive maintenance model

### Key Stats
- **File:** `ai4i2020.csv`
- **Target:** Machine failure (3.39% failure rate)
- **Features:** Temperature, RPM, Torque, Tool wear
- **Quality:** Perfect - no missing values
- **Use case:** Predict equipment failures

### Features
1. UDI - Unique Device Identifier
2. Product ID - Product code
3. Type - Quality variant (L/M/H)
4. Air temperature [K]
5. Process temperature [K]
6. Rotational speed [rpm]
7. Torque [Nm]
8. Tool wear [min]
9. Machine failure (target)
10. TWF, HDF, PWF, OSF, RNF (failure types)

---

## 🎯 Dataset 2: Quality Control (SECOM)

**Source:** UCI Machine Learning / Kaggle  
**Purpose:** Quality defect detection

### Key Stats
- **File:** `uci-secom.csv`
- **Target:** Pass/Fail classification
- **Features:** ~592 sensor measurements
- **Quality:** ⚠️ High missing values (typical for manufacturing sensors)
- **Challenge:** High dimensionality
- **Use case:** Predict manufacturing defects

### Data Characteristics
- Real-world manufacturing sensor data
- Many sensors malfunction → missing data
- Feature selection will be critical
- May use PCA for dimensionality reduction

### Cleaning Strategy
1. Remove features with >70% missing values
2. Impute remaining with median/KNN
3. Feature selection (correlation, importance)
4. Consider PCA to reduce to ~50 components

---

## 🚗 Dataset 3: Vehicle Specifications

**Source:** Kaggle - Car Dataset  
**Purpose:** Product dimension for warehouse

### Key Stats
- **File:** `data.csv`
- **Records:** 11,914 vehicles
- **Makes:** 48 unique manufacturers
- **Year range:** 1990-2017
- **Quality:** Some missing values (manageable)
- **Use case:** Product catalog, production planning

### Features
1. Make
2. Model
3. Year
4. Engine Fuel Type
5. Engine HP
6. Engine Cylinders
7. Transmission Type
8. Driven_Wheels
9. Number of Doors
10. Market Category
11. Vehicle Size
12. Vehicle Style
13. Highway MPG
14. City MPG
15. Popularity
16. MSRP

### Top 5 Makes
1. Chevrolet (1,123 models)
2. Ford (881 models)
3. Volkswagen (809 models)
4. Toyota (746 models)
5. Dodge (626 models)

---

## 📊 Dataset 4: Production Metrics (Synthetic)

**Status:** ⏳ To be generated on Day 5

### Planned Features
- Production lines (5 lines: A, B, C, D, E)
- Shifts (3 shifts/day: Morning, Afternoon, Night)
- Vehicle models (link to Dataset 3)
- Production quantities
- Cycle times
- Downtime events with causes
- Defect counts
- Rework counts
- OEE calculations (Availability × Performance × Quality)

### Generation Parameters
- **Duration:** 90 days historical data
- **Frequency:** Shift-level granularity
- **Lines:** 5 production lines
- **Target OEE:** ~75-80% (realistic manufacturing)
- **Defect rate:** 2-5% (realistic)

---

## 🎯 Data Warehouse Integration Plan

### Star Schema Design

**Fact Table:**
