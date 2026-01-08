#!/usr/bin/env python3
"""
Generate Synthetic Production Data for Stellantis Manufacturing Analytics

Business Context:
- Simulates 5 production lines over 90 days
- 3 shifts per day (Morning, Afternoon, Night)
- Realistic OEE calculations based on automotive industry standards
- Links to vehicle models from dataset 3

Author: Vanel
Date: Day 5
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ============================================================
# CONFIGURATION - Business Parameters
# ============================================================

# Seed for reproducibility (important pour debugging!)
# Si tu relances le script, tu auras les MÊMES données
np.random.seed(42)
random.seed(42)

# Business constants
DAYS = 90                    # 90 jours historiques (3 mois)
LINES = ['Line_A', 'Line_B', 'Line_C', 'Line_D', 'Line_E']  # 5 lignes
SHIFTS = [1, 2, 3]          # 1=Matin, 2=Après-midi, 3=Nuit
SHIFT_NAMES = ['Morning', 'Afternoon', 'Night']
SHIFT_DURATION = 8          # 8 heures par shift

# Vehicle models (from dataset 3 - realistic Stellantis brands)
VEHICLE_MODELS = [
    'Peugeot_208',
    'Peugeot_308', 
    'Citroen_C3',
    'Opel_Corsa',
    'Fiat_500'
]

# Cycle times par modèle (en minutes)
# Modèles plus gros = plus long à assembler
CYCLE_TIMES = {
    'Peugeot_208': 2.5,   # Petit modèle, rapide
    'Peugeot_308': 3.0,   # Moyenne gamme, plus complexe
    'Citroen_C3': 2.8,
    'Opel_Corsa': 2.6,
    'Fiat_500': 2.4       # Petit, le plus rapide
}

# Target OEE par ligne (certaines lignes sont plus vieilles)
LINE_OEE_TARGETS = {
    'Line_A': 0.75,  # Plus vieille ligne, OEE plus bas
    'Line_B': 0.80,  # Standard
    'Line_C': 0.82,  # Bonne ligne
    'Line_D': 0.78,  # Standard
    'Line_E': 0.85   # Ligne neuve, meilleur OEE
}

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def generate_downtime(shift, day_of_week, line):
    """
    Génère downtime réaliste en fonction de plusieurs facteurs
    
    Logique business:
    - Shift de nuit = plus de downtime (moins de staff, fatigue)
    - Lundi = plus de downtime (démarrage semaine, problèmes accumulés weekend)
    - Vieilles lignes (A, D) = plus de downtime
    
    Distribution: Gamma (skewed right, no negatives)
    - shape=2: Concentration vers petites valeurs
    - scale=15: Moyenne autour de 30 minutes
    """
    
    # Base downtime (distribution Gamma)
    if shift == 3:  # Nuit
        # Plus de pannes la nuit
        base_downtime = np.random.gamma(shape=2, scale=20)
    else:
        base_downtime = np.random.gamma(shape=2, scale=15)
    
    # Facteur lundi (20% plus de downtime)
    if day_of_week == 0:  # Lundi
        base_downtime *= 1.2
    
    # Facteur ligne (vieilles lignes = plus de pannes)
    if line in ['Line_A', 'Line_D']:
        base_downtime *= 1.15
    
    # Cap à 30% du shift (si plus, c'est arrêt complet, pas normal)
    max_downtime = SHIFT_DURATION * 60 * 0.3  # 30% de 480 minutes
    downtime = min(base_downtime, max_downtime)
    
    return round(downtime, 2)


def calculate_production(actual_time, cycle_time, performance_factor):
    """
    Calcule production réelle
    
    Logique:
    - Production théorique = temps disponible / cycle time
    - Production réelle = théorique × performance (toujours < 100%)
    
    Performance factor simule:
    - Ralentissements opérateurs
    - Micro-arrêts (pas comptés comme downtime)
    - Efficacité variable
    """
    if actual_time <= 0:
        return 0
    
    # Production théorique
    theoretical_production = actual_time / cycle_time
    
    # Production réelle (avec performance factor)
    actual_production = int(theoretical_production * performance_factor)
    
    return max(0, actual_production)  # Pas de production négative!


def generate_defects(production, base_defect_rate, shift):
    """
    Génère défauts qualité
    
    Logique:
    - Taux de défaut de base: 2-5% (standard automotive)
    - Shift de nuit: +30% défauts (fatigue opérateurs)
    - Distribution Beta (bounded entre 0 et 1)
    """
    if production == 0:
        return 0
    
    # Defect rate variable (distribution Beta - parfait pour taux entre 0-1)
    defect_rate = np.random.beta(2, 50)  # Moyenne ~3-4%
    
    # Ajustement shift nuit
    if shift == 3:
        defect_rate *= 1.3  # 30% plus de défauts la nuit
    
    # Nombre de défauts
    defect_count = int(production * defect_rate)
    
    # Cap réaliste (max 10% de défauts, sinon ligne arrêtée)
    max_defects = int(production * 0.10)
    
    return min(defect_count, max_defects)


def calculate_oee(availability, performance, quality):
    """
    Calcule OEE (Overall Equipment Effectiveness)
    
    Formule industrie standard:
    OEE = Availability × Performance × Quality
    
    Exemple:
    - Availability = 90% (ligne dispo 90% du temps)
    - Performance = 95% (produit à 95% de la vitesse théorique)
    - Quality = 97% (97% de bonnes pièces)
    → OEE = 0.90 × 0.95 × 0.97 = 83%
    """
    oee = (availability / 100) * (performance / 100) * (quality / 100) * 100
    return round(oee, 2)


# ============================================================
# MAIN GENERATION FUNCTION
# ============================================================

def generate_production_data(days=DAYS):
    """
    Génère toutes les données de production
    
    Structure:
    - Loop sur chaque jour
    - Loop sur chaque ligne
    - Loop sur chaque shift
    - Génère métriques pour cette combinaison
    """
    
    print("="*60)
    print("🏭 GENERATING SYNTHETIC PRODUCTION DATA")
    print("="*60)
    print(f"\nParameters:")
    print(f"  • Days: {days}")
    print(f"  • Lines: {len(LINES)}")
    print(f"  • Shifts per day: {len(SHIFTS)}")
    print(f"  • Expected records: {days * len(LINES) * len(SHIFTS)}")
    print("\nGenerating data...\n")
    
    data = []
    start_date = datetime.now() - timedelta(days=days)
    
    # Loop principal
    for day in range(days):
        current_date = start_date + timedelta(days=day)
        day_of_week = current_date.weekday()  # 0=Lundi, 6=Dimanche
        
        # Skip weekends (usine fermée - optionnel, mais réaliste)
        if day_of_week >= 5:  # Samedi/Dimanche
            continue
        
        for line in LINES:
            # Assigne un modèle de véhicule à cette ligne
            vehicle_model = VEHICLE_MODELS[LINES.index(line)]
            cycle_time = CYCLE_TIMES[vehicle_model]
            
            for shift in SHIFTS:
                # ==========================================
                # GÉNÉRATION DES MÉTRIQUES
                # ==========================================
                
                # 1. Temps planifié (constant)
                planned_time_minutes = SHIFT_DURATION * 60  # 480 minutes
                
                # 2. Downtime (variable, réaliste)
                downtime_minutes = generate_downtime(shift, day_of_week, line)
                
                # 3. Temps de production réel
                actual_time_minutes = planned_time_minutes - downtime_minutes
                
                # 4. Performance factor (variation aléatoire normale)
                # Moyenne 95%, écart-type 5%
                performance_factor = np.random.normal(0.95, 0.05)
                performance_factor = max(0.7, min(1.0, performance_factor))  # Clamp entre 70-100%
                
                # 5. Production
                expected_units = int(planned_time_minutes / cycle_time)  # Production théorique
                actual_units = calculate_production(actual_time_minutes, cycle_time, performance_factor)
                
                # 6. Défauts qualité
                defect_count = generate_defects(actual_units, 0.03, shift)
                good_units = actual_units - defect_count
                
                # 7. Rework vs Scrap
                # 70% des défauts peuvent être réparés, 30% sont à jeter
                rework_count = int(defect_count * 0.7)
                scrap_count = defect_count - rework_count
                
                # ==========================================
                # CALCUL OEE
                # ==========================================
                
                # Availability (% temps disponible)
                availability_pct = (actual_time_minutes / planned_time_minutes) * 100
                
                # Performance (% vitesse théorique)
                if expected_units > 0:
                    performance_pct = (actual_units / expected_units) * 100
                else:
                    performance_pct = 0
                
                # Quality (% bonnes pièces)
                if actual_units > 0:
                    quality_pct = (good_units / actual_units) * 100
                else:
                    quality_pct = 0
                
                # OEE final
                oee_score = calculate_oee(availability_pct, performance_pct, quality_pct)
                
                # ==========================================
                # CAUSES DE DOWNTIME (réaliste)
                # ==========================================
                downtime_causes = [
                    'Setup_Changeover',      # Changement d'outillage
                    'Maintenance_Preventive', # Maintenance planifiée
                    'Material_Shortage',     # Manque de pièces
                    'Equipment_Failure',     # Panne équipement
                    'Quality_Issue',         # Problème qualité
                    'Operator_Absence'       # Manque d'opérateurs
                ]
                primary_downtime_cause = random.choice(downtime_causes)
                
                # ==========================================
                # CYCLE TIME RÉEL (peut varier légèrement)
                # ==========================================
                if actual_units > 0:
                    actual_cycle_time = actual_time_minutes / actual_units
                else:
                    actual_cycle_time = 0
                
                # ==========================================
                # AUTRES MÉTRIQUES
                # ==========================================
                operators_count = random.randint(8, 12)  # 8-12 opérateurs par shift
                
                # Line speed (unités par heure)
                if actual_cycle_time > 0:
                    line_speed = round(60 / actual_cycle_time, 2)
                else:
                    line_speed = 0
                
                # ==========================================
                # RECORD COMPLET
                # ==========================================
                record = {
                    # Identifiants
                    'date': current_date.strftime('%Y-%m-%d'),
                    'production_line': line,
                    'shift': shift,
                    'shift_name': SHIFT_NAMES[shift-1],
                    'vehicle_model': vehicle_model,
                    
                    # Temps
                    'planned_production_time_min': planned_time_minutes,
                    'downtime_min': downtime_minutes,
                    'actual_production_time_min': actual_time_minutes,
                    
                    # Production
                    'expected_units': expected_units,
                    'actual_units': actual_units,
                    'good_units': good_units,
                    
                    # Qualité
                    'defect_count': defect_count,
                    'rework_count': rework_count,
                    'scrap_count': scrap_count,
                    
                    # Cycle times
                    'target_cycle_time_min': cycle_time,
                    'actual_cycle_time_min': round(actual_cycle_time, 2),
                    
                    # OEE Components
                    'availability_pct': round(availability_pct, 2),
                    'performance_pct': round(performance_pct, 2),
                    'quality_pct': round(quality_pct, 2),
                    'oee_score': oee_score,
                    
                    # Contexte
                    'primary_downtime_cause': primary_downtime_cause,
                    'operators_count': operators_count,
                    'line_speed_units_per_hour': line_speed
                }
                
                data.append(record)
    
    # Créer DataFrame
    df = pd.DataFrame(data)
    
    return df


# ============================================================
# QUALITY CHECKS
# ============================================================

def validate_data(df):
    """
    Vérifie que les données générées sont cohérentes
    
    Checks:
    1. Pas de valeurs négatives
    2. OEE entre 0-100%
    3. Defects <= Production
    4. Downtime <= Planned time
    """
    print("\n" + "="*60)
    print("✅ DATA QUALITY CHECKS")
    print("="*60)
    
    issues = []
    
    # Check 1: Valeurs négatives
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    negative_cols = [col for col in numeric_cols if (df[col] < 0).any()]
    if negative_cols:
        issues.append(f"❌ Negative values in: {negative_cols}")
    else:
        print("✅ No negative values")
    
    # Check 2: OEE range
    if ((df['oee_score'] < 0) | (df['oee_score'] > 100)).any():
        issues.append("❌ OEE outside 0-100% range")
    else:
        print("✅ OEE within valid range (0-100%)")
    
    # Check 3: Defects <= Production
    if (df['defect_count'] > df['actual_units']).any():
        issues.append("❌ Defects > Production (impossible!)")
    else:
        print("✅ Defects ≤ Production")
    
    # Check 4: Downtime <= Planned time
    if (df['downtime_min'] > df['planned_production_time_min']).any():
        issues.append("❌ Downtime > Planned time (impossible!)")
    else:
        print("✅ Downtime ≤ Planned time")
    
    # Check 5: Good units cohérence
    calculated_good = df['actual_units'] - df['defect_count']
    if not (df['good_units'] == calculated_good).all():
        issues.append("❌ Good units calculation incorrect")
    else:
        print("✅ Good units = Production - Defects")
    
    if issues:
        print("\n⚠️ ISSUES FOUND:")
        for issue in issues:
            print(f"  {issue}")
        return False
    else:
        print("\n✅ ALL CHECKS PASSED!")
        return True


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Point d'entrée principal"""
    
    # Génération
    df = generate_production_data(days=DAYS)
    
    # Validation
    is_valid = validate_data(df)
    
    if not is_valid:
        print("\n⚠️ Data validation failed. Review generation logic.")
        return
    
    # Save to CSV
    output_path = '/home/vanel/stellantis-manufacturing-analytics/data/raw/production/production_metrics.csv'
    df.to_csv(output_path, index=False)
    
    print("\n" + "="*60)
    print("📊 GENERATION SUMMARY")
    print("="*60)
    print(f"\n✅ Generated {len(df):,} production records")
    print(f"📁 Saved to: {output_path}")
    
    print(f"\n📅 Date range:")
    print(f"   From: {df['date'].min()}")
    print(f"   To: {df['date'].max()}")
    
    print(f"\n🏭 Production lines: {df['production_line'].nunique()}")
    print(f"🚗 Vehicle models: {df['vehicle_model'].nunique()}")
    
    print(f"\n📊 Key Statistics:")
    print(f"   Average OEE: {df['oee_score'].mean():.2f}%")
    print(f"   OEE Range: {df['oee_score'].min():.2f}% - {df['oee_score'].max():.2f}%")
    print(f"   Total units produced: {df['actual_units'].sum():,}")
    print(f"   Total defects: {df['defect_count'].sum():,}")
    print(f"   Overall defect rate: {(df['defect_count'].sum() / df['actual_units'].sum() * 100):.2f}%")
    print(f"   Average downtime: {df['downtime_min'].mean():.2f} minutes/shift")
    
    print(f"\n🎯 OEE by Line:")
    for line in LINES:
        line_oee = df[df['production_line'] == line]['oee_score'].mean()
        target = LINE_OEE_TARGETS[line] * 100
        diff = line_oee - target
        status = "✅" if diff >= 0 else "⚠️"
        print(f"   {line}: {line_oee:.2f}% (target: {target:.0f}%) {status}")
    
    print(f"\n📈 Top 5 Downtime Causes:")
    top_causes = df['primary_downtime_cause'].value_counts().head(5)
    for cause, count in top_causes.items():
        pct = (count / len(df)) * 100
        print(f"   {cause}: {count} times ({pct:.1f}%)")
    
    print("\n" + "="*60)
    print("✅ DAY 5 COMPLETE!")
    print("="*60)
    print("\n💡 Next steps:")
    print("   1. Explore data: python explore_all_datasets.py")
    print("   2. Git commit: git add . && git commit -m 'DAY 5'")
    print("   3. Move to Day 6: Data quality report")


if __name__ == "__main__":
    main()
