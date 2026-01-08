#!/usr/bin/env python3
"""Quick exploration of all 3 datasets"""

import pandas as pd
import numpy as np

print("="*60)
print("📊 DATASET EXPLORATION - DAY 4")
print("="*60)

# ========================================
# DATASET 1: MAINTENANCE
# ========================================
print("\n🔧 DATASET 1: PREDICTIVE MAINTENANCE")
print("-"*60)

try:
    df_maint = pd.read_csv('data/raw/maintenance/ai4i2020.csv')
    print(f"✅ Loaded: {df_maint.shape[0]:,} rows × {df_maint.shape[1]} columns")
    print(f"   Missing values: {df_maint.isnull().sum().sum()}")
    print(f"   Failures: {df_maint['Machine failure'].sum()} ({df_maint['Machine failure'].mean()*100:.2f}%)")
except Exception as e:
    print(f"❌ Error: {e}")

# ========================================
# DATASET 2: QUALITY (SECOM)
# ========================================
print("\n🎯 DATASET 2: QUALITY CONTROL (SECOM)")
print("-"*60)

try:
    # Le fichier est uci-secom.csv (un seul fichier combiné)
    df_quality = pd.read_csv('data/raw/quality/uci-secom.csv')
    
    print(f"✅ Data loaded: {df_quality.shape[0]:,} rows × {df_quality.shape[1]} columns")
    print(f"   Missing values: {df_quality.isnull().sum().sum():,} ({df_quality.isnull().sum().sum()/(df_quality.shape[0]*df_quality.shape[1])*100:.1f}%)")
    
    # Vérifie si il y a une colonne target
    print(f"   Columns: {list(df_quality.columns[:5])}... (showing first 5)")
    
    # Si dernière colonne est le label
    last_col = df_quality.columns[-1]
    if df_quality[last_col].nunique() <= 2:
        print(f"   Target column: {last_col}")
        print(f"   Distribution: {df_quality[last_col].value_counts().to_dict()}")
    
except Exception as e:
    print(f"❌ Error: {e}")

# ========================================
# DATASET 3: VEHICLES
# ========================================
print("\n🚗 DATASET 3: VEHICLE SPECIFICATIONS")
print("-"*60)

try:
    df_vehicles = pd.read_csv('data/raw/vehicles/data.csv')
    print(f"✅ Loaded: {df_vehicles.shape[0]:,} rows × {df_vehicles.shape[1]} columns")
    print(f"   Missing values: {df_vehicles.isnull().sum().sum():,}")
    print(f"   Unique makes: {df_vehicles['Make'].nunique()}")
    print(f"   Year range: {df_vehicles['Year'].min()} - {df_vehicles['Year'].max()}")
    print(f"\n   Top 5 makes:")
    for make, count in df_vehicles['Make'].value_counts().head(5).items():
        print(f"     {make}: {count}")
except Exception as e:
    print(f"❌ Error: {e}")

# ========================================
# SUMMARY
# ========================================
print("\n" + "="*60)
print("📈 SUMMARY - ALL DATASETS")
print("="*60)

try:
    total_rows = df_maint.shape[0] + df_quality.shape[0] + df_vehicles.shape[0]
    print(f"\n✅ Total data: {total_rows:,} rows across 3 datasets")
    print(f"\n📊 Dataset sizes:")
    print(f"   1. Maintenance:  {df_maint.shape[0]:>6,} rows × {df_maint.shape[1]:>3} cols")
    print(f"   2. Quality:      {df_quality.shape[0]:>6,} rows × {df_quality.shape[1]:>3} cols")
    print(f"   3. Vehicles:     {df_vehicles.shape[0]:>6,} rows × {df_vehicles.shape[1]:>3} cols")
    
    print(f"\n💡 Key observations:")
    print(f"   • Maintenance: Clean dataset, ready for ML")
    print(f"   • Quality: High dimensionality, needs feature selection")
    print(f"   • Vehicles: Rich catalog for production planning")
    
    print("\n🚀 All 3 datasets ready for pipeline!")
except Exception as e:
    print(f"⚠️ Could not generate summary")

print("="*60)
print("✅ EXPLORATION COMPLETE - DAY 4")
print("="*60)
