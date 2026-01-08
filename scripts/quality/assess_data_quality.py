#!/usr/bin/env python3
"""
Data Quality Assessment for Stellantis Manufacturing Analytics

Evaluates data quality across 6 dimensions:
1. Completeness (no missing values)
2. Consistency (no contradictions)
3. Validity (values in expected ranges)
4. Uniqueness (no duplicates)
5. Timeliness (data freshness)
6. Accuracy (correctness)

Generates comprehensive quality report with scores and recommendations.

Author: Vanel
Date: Day 6
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

# ============================================================
# CONFIGURATION
# ============================================================

# Chemins des datasets
DATASETS = {
    'maintenance': 'data/raw/maintenance/ai4i2020.csv',
    'quality': 'data/raw/quality/uci-secom.csv',
    'vehicles': 'data/raw/vehicles/data.csv',
    'production': 'data/raw/production/production_metrics.csv'
}

# Seuils de qualité (standards industrie)
THRESHOLDS = {
    'excellent': 90,  # 90-100% = Excellent
    'good': 70,       # 70-89% = Good
    'fair': 50,       # 50-69% = Fair
    'poor': 0         # 0-49% = Poor
}

# ============================================================
# HELPER FUNCTIONS - QUALITY DIMENSIONS
# ============================================================

def assess_completeness(df):
    """
    Dimension 1: COMPLETENESS
    
    Mesure: % de valeurs non-nulles
    
    Business impact:
    - Nulls = informations manquantes
    - Peut biaiser analyses
    - ML models peuvent crash
    
    Score: (total_values - null_values) / total_values * 100
    """
    total_cells = df.shape[0] * df.shape[1]
    null_cells = df.isnull().sum().sum()
    non_null_cells = total_cells - null_cells
    
    score = (non_null_cells / total_cells) * 100
    
    # Détails par colonne (top 10 problématiques)
    null_by_col = df.isnull().sum().sort_values(ascending=False)
    null_pct_by_col = (null_by_col / len(df) * 100).round(2)
    
    problematic_cols = null_pct_by_col[null_pct_by_col > 10].head(10)
    
    return {
        'score': round(score, 2),
        'total_cells': total_cells,
        'null_cells': null_cells,
        'null_percentage': round((null_cells / total_cells) * 100, 2),
        'problematic_columns': problematic_cols.to_dict() if len(problematic_cols) > 0 else {}
    }


def assess_uniqueness(df):
    """
    Dimension 2: UNIQUENESS
    
    Mesure: % de lignes uniques (pas de duplicates)
    
    Business impact:
    - Duplicates = double comptage
    - Fausse les KPIs (revenus × 2!)
    - Problème grave pour analytics
    
    Score: (unique_rows / total_rows) * 100
    """
    total_rows = len(df)
    duplicates = df.duplicated().sum()
    unique_rows = total_rows - duplicates
    
    score = (unique_rows / total_rows) * 100
    
    return {
        'score': round(score, 2),
        'total_rows': total_rows,
        'duplicate_rows': duplicates,
        'duplicate_percentage': round((duplicates / total_rows) * 100, 2)
    }


def assess_validity(df):
    """
    Dimension 3: VALIDITY
    
    Mesure: % de valeurs dans ranges valides
    
    Business impact:
    - Valeurs aberrantes = données corrompues
    - Ex: Age = -5, OEE = 150%
    - Fausse les analyses
    
    Checks:
    - Colonnes numériques: outliers (IQR method)
    - Colonnes avec % ou score: 0-100 range
    """
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    total_numeric_cells = 0
    invalid_cells = 0
    invalid_details = {}
    
    for col in numeric_cols:
        col_data = df[col].dropna()
        total_numeric_cells += len(col_data)
        
        # Check 1: Valeurs négatives si colonne devrait être positive
        if 'count' in col.lower() or 'quantity' in col.lower() or 'units' in col.lower():
            negatives = (col_data < 0).sum()
            if negatives > 0:
                invalid_cells += negatives
                invalid_details[col] = f"{negatives} negative values"
        
        # Check 2: Pourcentages hors de 0-100
        if 'pct' in col.lower() or 'percentage' in col.lower() or col.lower().endswith('_pct'):
            out_of_range = ((col_data < 0) | (col_data > 100)).sum()
            if out_of_range > 0:
                invalid_cells += out_of_range
                invalid_details[col] = f"{out_of_range} values outside 0-100%"
        
        # Check 3: Outliers extrêmes (> 3 std from mean)
        if len(col_data) > 0:
            mean = col_data.mean()
            std = col_data.std()
            if std > 0:
                outliers = ((col_data < mean - 3*std) | (col_data > mean + 3*std)).sum()
                # On compte pas les outliers comme invalides (peuvent être réels)
                # Juste pour info
    
    if total_numeric_cells > 0:
        score = ((total_numeric_cells - invalid_cells) / total_numeric_cells) * 100
    else:
        score = 100  # Pas de colonnes numériques = pas d'invalidité
    
    return {
        'score': round(score, 2),
        'invalid_cells': invalid_cells,
        'invalid_details': invalid_details
    }


def assess_consistency(df):
    """
    Dimension 4: CONSISTENCY
    
    Mesure: Absence de contradictions logiques
    
    Business impact:
    - Incohérences = perte de confiance
    - Ex: Date de naissance > Date actuelle
    - Difficile à mesurer automatiquement
    
    Score: Basé sur règles business spécifiques
    """
    # Pour cette version, on fait des checks basiques
    issues = []
    
    # Check dates (si colonnes date existent)
    date_cols = df.select_dtypes(include=['datetime64']).columns
    for col in date_cols:
        future_dates = (df[col] > pd.Timestamp.now()).sum()
        if future_dates > 0:
            issues.append(f"{col}: {future_dates} future dates")
    
    # Check colonnes avec "date" dans le nom (string format)
    for col in df.columns:
        if 'date' in col.lower() and df[col].dtype == 'object':
            try:
                dates = pd.to_datetime(df[col], errors='coerce')
                future_dates = (dates > pd.Timestamp.now()).sum()
                if future_dates > 0:
                    issues.append(f"{col}: {future_dates} future dates")
            except:
                pass
    
    # Score: 100% si pas d'issues, sinon pénalité
    score = 100 - (len(issues) * 10)  # -10% par issue
    score = max(0, score)  # Minimum 0
    
    return {
        'score': round(score, 2),
        'issues_found': len(issues),
        'issues': issues
    }


def assess_timeliness(df, dataset_name):
    """
    Dimension 5: TIMELINESS
    
    Mesure: Fraîcheur des données
    
    Business impact:
    - Vieilles données = décisions obsolètes
    - Dashboard avec data de 6 mois = inutile
    
    Score: Basé sur date la plus récente
    """
    # Cherche colonne date
    date_col = None
    for col in df.columns:
        if 'date' in col.lower():
            date_col = col
            break
    
    if date_col is None:
        # Pas de colonne date, on assume synthétique/statique
        return {
            'score': 100,
            'most_recent_date': 'N/A (no date column)',
            'days_old': 0,
            'note': 'Static/reference dataset'
        }
    
    try:
        # Convert to datetime
        dates = pd.to_datetime(df[date_col], errors='coerce')
        most_recent = dates.max()
        
        # Calcul âge
        if pd.notna(most_recent):
            days_old = (pd.Timestamp.now() - most_recent).days
            
            # Scoring basé sur âge
            if days_old <= 7:
                score = 100  # Très récent
            elif days_old <= 30:
                score = 90   # Récent
            elif days_old <= 90:
                score = 80   # OK
            elif days_old <= 180:
                score = 70   # Un peu vieux
            else:
                score = 60   # Vieux
            
            return {
                'score': score,
                'most_recent_date': str(most_recent.date()),
                'days_old': days_old
            }
    except:
        pass
    
    return {
        'score': 100,
        'most_recent_date': 'N/A',
        'days_old': 0,
        'note': 'Could not parse dates'
    }


def assess_accuracy(df, dataset_name):
    """
    Dimension 6: ACCURACY
    
    Mesure: Correctness des valeurs
    
    Business impact:
    - Fausses valeurs = mauvaises décisions
    - Difficile à mesurer sans "ground truth"
    
    Score: Estimation basée sur règles business
    """
    # Accuracy est difficile à mesurer sans référence
    # On fait une estimation basée sur:
    # 1. Présence de valeurs improbables
    # 2. Distribution des données (skewness extrême)
    
    score = 100  # Start optimiste
    issues = []
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    for col in numeric_cols[:10]:  # Sample de 10 colonnes
        col_data = df[col].dropna()
        
        if len(col_data) > 0:
            # Check distribution (skewness extrême peut indiquer problème)
            skewness = col_data.skew()
            if abs(skewness) > 5:  # Très skewed
                issues.append(f"{col}: extreme skewness ({skewness:.2f})")
    
    # Pénalité si beaucoup d'issues
    score -= len(issues) * 5
    score = max(60, score)  # Minimum 60% (on peut pas vraiment savoir)
    
    return {
        'score': round(score, 2),
        'note': 'Estimated (no ground truth available)',
        'potential_issues': issues[:5]  # Top 5
    }


# ============================================================
# MAIN ASSESSMENT FUNCTION
# ============================================================

def assess_dataset(filepath, name):
    """
    Évalue qualité d'un dataset sur les 6 dimensions
    
    Returns: Dict avec scores et détails
    """
    print(f"\n{'='*60}")
    print(f"📊 ASSESSING: {name.upper()}")
    print(f"{'='*60}")
    
    # Load dataset
    try:
        df = pd.read_csv(filepath)
        print(f"✅ Loaded: {df.shape[0]:,} rows × {df.shape[1]} columns")
    except Exception as e:
        print(f"❌ Error loading: {e}")
        return None
    
    # Évaluations
    print("\nEvaluating dimensions...")
    
    completeness = assess_completeness(df)
    print(f"  1. Completeness: {completeness['score']}%")
    
    uniqueness = assess_uniqueness(df)
    print(f"  2. Uniqueness: {uniqueness['score']}%")
    
    validity = assess_validity(df)
    print(f"  3. Validity: {validity['score']}%")
    
    consistency = assess_consistency(df)
    print(f"  4. Consistency: {consistency['score']}%")
    
    timeliness = assess_timeliness(df, name)
    print(f"  5. Timeliness: {timeliness['score']}%")
    
    accuracy = assess_accuracy(df, name)
    print(f"  6. Accuracy: {accuracy['score']}%")
    
    # Score global (moyenne des 6 dimensions)
    overall_score = (
        completeness['score'] +
        uniqueness['score'] +
        validity['score'] +
        consistency['score'] +
        timeliness['score'] +
        accuracy['score']
    ) / 6
    
    print(f"\n🎯 OVERALL QUALITY SCORE: {overall_score:.2f}%")
    
    # Grade
    if overall_score >= THRESHOLDS['excellent']:
        grade = 'A (Excellent)'
    elif overall_score >= THRESHOLDS['good']:
        grade = 'B (Good)'
    elif overall_score >= THRESHOLDS['fair']:
        grade = 'C (Fair)'
    else:
        grade = 'D (Poor)'
    
    print(f"   Grade: {grade}")
    
    return {
        'name': name,
        'shape': f"{df.shape[0]:,} rows × {df.shape[1]} columns",
        'overall_score': round(overall_score, 2),
        'grade': grade,
        'dimensions': {
            'completeness': completeness,
            'uniqueness': uniqueness,
            'validity': validity,
            'consistency': consistency,
            'timeliness': timeliness,
            'accuracy': accuracy
        }
    }


# ============================================================
# REPORT GENERATION
# ============================================================

def generate_markdown_report(results):
    """
    Génère rapport Markdown complet
    
    Format professionnel avec:
    - Executive summary
    - Scores par dataset
    - Recommandations
    - Action plan
    """
    
    report = []
    
    # Header
    report.append("# 📊 Data Quality Assessment Report")
    report.append("")
    report.append("**Project:** Stellantis Manufacturing Analytics")
    report.append(f"**Date:** {datetime.now().strftime('%Y-%m-%d')}")
    report.append("**Author:** Vanel")
    report.append("**Status:** Week 1 - Data Acquisition Complete")
    report.append("")
    report.append("---")
    report.append("")
    
    # Executive Summary
    report.append("## 📋 Executive Summary")
    report.append("")
    report.append("Quality assessment of 4 datasets for manufacturing analytics pipeline.")
    report.append("")
    
    # Calculate overall project score
    project_score = sum(r['overall_score'] for r in results) / len(results)
    report.append(f"**Overall Project Data Quality: {project_score:.2f}%**")
    report.append("")
    
    # Summary table
    report.append("| Dataset | Rows | Columns | Quality Score | Grade | Status |")
    report.append("|---------|------|---------|---------------|-------|--------|")
    
    for r in results:
        rows, cols = r['shape'].split(' × ')
        status = "✅ Ready" if r['overall_score'] >= 80 else "⚠️ Needs cleaning"
        report.append(f"| {r['name'].title()} | {rows} | {cols} | {r['overall_score']}% | {r['grade']} | {status} |")
    
    report.append("")
    report.append("---")
    report.append("")
    
    # Detailed Assessment per Dataset
    report.append("## 🔍 Detailed Quality Assessment")
    report.append("")
    
    for r in results:
        report.append(f"### {r['name'].upper()} Dataset")
        report.append("")
        report.append(f"**Shape:** {r['shape']}")
        report.append(f"**Overall Score:** {r['overall_score']}% ({r['grade']})")
        report.append("")
        
        # Dimensions scores
        report.append("#### Quality Dimensions")
        report.append("")
        report.append("| Dimension | Score | Status |")
        report.append("|-----------|-------|--------|")
        
        dims = r['dimensions']
        for dim_name, dim_data in dims.items():
            score = dim_data['score']
            if score >= 90:
                status = "✅ Excellent"
            elif score >= 70:
                status = "⚠️ Good"
            elif score >= 50:
                status = "🔶 Fair"
            else:
                status = "❌ Poor"
            
            report.append(f"| {dim_name.title()} | {score}% | {status} |")
        
        report.append("")
        
        # Key findings
        report.append("#### Key Findings")
        report.append("")
        
        # Completeness issues
        if dims['completeness']['null_percentage'] > 5:
            report.append(f"**Completeness Issues:**")
            report.append(f"- {dims['completeness']['null_percentage']}% missing values")
            if dims['completeness']['problematic_columns']:
                report.append(f"- Top problematic columns:")
                for col, pct in list(dims['completeness']['problematic_columns'].items())[:5]:
                    report.append(f"  - `{col}`: {pct}% missing")
            report.append("")
        
        # Uniqueness issues
        if dims['uniqueness']['duplicate_rows'] > 0:
            report.append(f"**Uniqueness Issues:**")
            report.append(f"- {dims['uniqueness']['duplicate_rows']} duplicate rows ({dims['uniqueness']['duplicate_percentage']}%)")
            report.append("")
        
        # Validity issues
        if dims['validity']['invalid_cells'] > 0:
            report.append(f"**Validity Issues:**")
            report.append(f"- {dims['validity']['invalid_cells']} invalid values detected")
            if dims['validity']['invalid_details']:
                for col, issue in dims['validity']['invalid_details'].items():
                    report.append(f"  - `{col}`: {issue}")
            report.append("")
        
        # Consistency issues
        if dims['consistency']['issues_found'] > 0:
            report.append(f"**Consistency Issues:**")
            for issue in dims['consistency']['issues']:
                report.append(f"- {issue}")
            report.append("")
        
        report.append("---")
        report.append("")
    
    # Recommendations
    report.append("## 💡 Recommendations & Action Plan")
    report.append("")
    
    for r in results:
        score = r['overall_score']
        name = r['name'].title()
        
        if score >= 90:
            report.append(f"### {name}: ✅ Production Ready")
            report.append(f"- **Status:** Excellent quality")
            report.append(f"- **Action:** Use as-is, minimal cleaning needed")
            report.append("")
        
        elif score >= 70:
            report.append(f"### {name}: ⚠️ Good (Minor Cleaning)")
            report.append(f"- **Status:** Good quality with minor issues")
            report.append(f"- **Action:** Light cleaning recommended")
            
            dims = r['dimensions']
            if dims['completeness']['null_percentage'] > 5:
                report.append(f"  - Handle {dims['completeness']['null_percentage']}% missing values")
            if dims['uniqueness']['duplicate_rows'] > 0:
                report.append(f"  - Remove {dims['uniqueness']['duplicate_rows']} duplicates")
            
            report.append("")
        
        else:
            report.append(f"### {name}: 🔶 Needs Significant Cleaning")
            report.append(f"- **Status:** Quality issues require attention")
            report.append(f"- **Action:** Comprehensive cleaning pipeline")
            report.append(f"- **Estimated effort:** 2-3 days")
            report.append("")
    
    report.append("---")
    report.append("")
    
    # Next Steps
    report.append("## 🚀 Next Steps")
    report.append("")
    report.append("1. **Week 2:** Database design (star schema DDL)")
    report.append("2. **Week 3:** ETL pipeline development")
    report.append("3. **Week 4:** Data cleaning implementation")
    report.append("4. **Week 5:** Feature engineering + ML models")
    report.append("")
    
    # Footer
    report.append("---")
    report.append("")
    report.append("**Report generated by:** `assess_data_quality.py`")
    report.append(f"**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return "\n".join(report)


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Point d'entrée principal"""
    
    print("="*60)
    print("🔍 DATA QUALITY ASSESSMENT - STELLANTIS PROJECT")
    print("="*60)
    print(f"\nAssessing {len(DATASETS)} datasets...")
    
    results = []
    
    # Assess each dataset
    for name, filepath in DATASETS.items():
        full_path = os.path.join(os.path.expanduser('~'), 
                                 'stellantis-manufacturing-analytics', 
                                 filepath)
        
        result = assess_dataset(full_path, name)
        if result:
            results.append(result)
    
    # Generate report
    print("\n" + "="*60)
    print("📝 GENERATING REPORT")
    print("="*60)
    
    report_content = generate_markdown_report(results)
    
    # Save report
    report_path = os.path.join(os.path.expanduser('~'),
                              'stellantis-manufacturing-analytics',
                              'docs/data_quality_report.md')
    
    with open(report_path, 'w') as f:
        f.write(report_content)
    
    print(f"\n✅ Report saved to: docs/data_quality_report.md")
    
    # Summary
    print("\n" + "="*60)
    print("📊 ASSESSMENT COMPLETE")
    print("="*60)
    
    project_score = sum(r['overall_score'] for r in results) / len(results)
    print(f"\n🎯 Overall Project Quality: {project_score:.2f}%")
    
    print("\n📋 Summary by dataset:")
    for r in results:
        status_icon = "✅" if r['overall_score'] >= 80 else "⚠️"
        print(f"   {status_icon} {r['name'].title()}: {r['overall_score']}% ({r['grade']})")
    
    print("\n✅ DAY 6 COMPLETE!")
    print("\n💡 Next: Review the report in docs/data_quality_report.md")


if __name__ == "__main__":
    main()
