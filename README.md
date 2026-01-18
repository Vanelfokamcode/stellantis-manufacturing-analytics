# 🚗 Stellantis Manufacturing Performance Analytics

> End-to-end data pipeline for automotive manufacturing analytics with predictive maintenance

## 🎯 Project Overview

Production-grade data engineering project built for **Stellantis Vehicle Productivity Team**, featuring:
- Predictive maintenance using Machine Learning
- Real-time OEE (Overall Equipment Effectiveness) monitoring
- Quality control analytics
- Interactive Power BI dashboards

---

## 🏗️ Architecture
```
Raw Data → Python ETL → PostgreSQL DWH → dbt → Power BI
              ↓
         ML Predictions
              ↓
    Apache Airflow (Orchestration)
```

---

## 🛠️ Tech Stack

- **Languages:** Python, SQL
- **Database:** PostgreSQL
- **Orchestration:** Apache Airflow
- **Transformation:** dbt
- **ML:** scikit-learn
- **BI:** Power BI

---

## 📁 Project Structure
```
stellantis-manufacturing-analytics/
├── data/              # Datasets (raw & processed)
├── scripts/           # ETL, ML, utilities
├── sql/               # Database schemas
├── notebooks/         # Data exploration
├── dags/              # Airflow DAGs
├── dbt_project/       # dbt transformations
├── dashboards/        # Power BI files
├── config/            # Configuration files
├── tests/             # Unit tests
└── docs/              # Documentation
```

---

## 📊 Datasets

1. **Predictive Maintenance** - Equipment sensor data (10K rows)
2. **Quality Control** - Manufacturing process metrics (1.5K rows)
3. **Vehicle Specs** - Car models database (11K rows)
4. **Production Metrics** - Synthetic production data (1.3K rows)

---

## 🚀 Quick Start
```bash
# Clone repository
git clone https://github.com/Vanelfokamcode/stellantis-manufacturing-analytics.git

# Create virtual environment
python3 -m venv venv_stellantis
source venv_stellantis/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## 📈 Project Progress

**Day 1/45 - IN PROGRESS** 🚧
- [x] Project structure created
- [x] Virtual environment setup
- [x] Dependencies installed
- [ ] Datasets download (Day 2)

---

## 💼 Business Value

- **15-20%** reduction in equipment downtime
- **5-8%** increase in OEE
- **30%** reduction in quality defects
- **Real-time** decision-making dashboards



## 📈 Project Progress
```
[████████░░░░░░░░░░░░] 35% Complete (Week 1 Done!)

✅ Week 1: Data Acquisition (Days 1-7) - COMPLETE
⏳ Week 2: Database Design (Days 8-14)
⏳ Week 3-4: ETL Pipeline (Days 15-28)
⏳ Week 5-6: ML & Warehouse (Days 29-42)
⏳ Week 7+: Airflow + dbt + Power BI (Days 43-45)
```

### Current Status: Week 1 Complete ✅

**Latest Milestone:** Data acquisition and quality assessment finished
- 4 datasets acquired (25K+ rows)
- Quality score: 97.6%
- All documentation complete
- Ready for database design

---

## 🚀 Quick Start

### Prerequisites
- Python 3.12+
- PostgreSQL 16+ (for Week 2+)
- Git
- WSL/Linux (recommended) or macOS

### Setup
```bash
# Clone repository
git clone https://github.com/Vanelfokamcode/stellantis-manufacturing-analytics.git
cd stellantis-manufacturing-analytics

# Create virtual environment
python3 -m venv venv_stellantis
source venv_stellantis/bin/activate  # On Windows: venv_stellantis\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Explore datasets
python explore_all_datasets.py

# Generate production data
python scripts/synthetic/generate_production_data.py

# Assess data quality
python scripts/quality/assess_data_quality.py
```

---

## 📊 Datasets Overview

**4 datasets totaling 24,831 rows:**

### 1. Predictive Maintenance (10,000 rows)
- **Source:** Kaggle - AI4I 2020
- **Purpose:** ML predictive maintenance model
- **Quality:** 96.67% (Excellent)
- **Features:** 14 (temperature, RPM, torque, tool wear, failure types)
- **Key Insight:** 3.39% failure rate, 5 failure types identified

### 2. Quality Control - SECOM (1,567 rows)
- **Source:** UCI Machine Learning / Kaggle
- **Purpose:** Manufacturing defect detection
- **Quality:** 97.58% (Excellent)
- **Features:** 592 (sensor measurements)
- **Key Insight:** High dimensionality, feature selection recommended

### 3. Vehicle Specifications (11,914 rows)
- **Source:** Kaggle - Car Dataset
- **Purpose:** Product catalog dimension
- **Quality:** 96.16% (Excellent)
- **Features:** 16 (make, model, year, specs, pricing)
- **Key Insight:** 48 makes, 1990-2017, rich for analytics

### 4. Production Metrics (1,350 rows)
- **Source:** Self-generated (synthetic)
- **Purpose:** Manufacturing production data
- **Quality:** 100% (Perfect)
- **Features:** 23 (OEE, downtime, quality, production volumes)
- **Key Insight:** Realistic OEE ~78%, based on industry standards

**Overall Data Quality Score: 97.6%** ✅

---
