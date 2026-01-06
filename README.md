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
git clone https://github.com/[your-username]/stellantis-manufacturing-analytics.git

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
