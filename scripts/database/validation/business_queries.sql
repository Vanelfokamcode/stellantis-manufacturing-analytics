-- ================================================================
-- STELLANTIS MANUFACTURING ANALYTICS
-- Script: Business Query Examples
-- Author: Vanel
-- Date: January 2025
-- Description: 10 real business questions answered by star schema
-- Business: Demonstrate value of the data warehouse
-- ================================================================

\set QUIET on
\pset border 2
\pset format wrapped

\echo ''
\echo '============================================================'
\echo 'BUSINESS QUERY EXAMPLES'
\echo 'Stellantis Manufacturing Analytics'
\echo '============================================================'
\echo ''

SET search_path TO warehouse;

-- ================================================================
-- QUERY 1: Vehicle Catalog Overview
-- Business Question: "What vehicles are we producing?"
-- ================================================================

\echo '🚗 QUERY 1: Top 10 Vehicle Makes by Model Count'
\echo '------------------------------------------------------------'

SELECT 
    make,
    COUNT(*) as model_count,
    ROUND(AVG(msrp), 0) as avg_price,
    MIN(year) as first_year,
    MAX(year) as last_year
FROM dim_vehicle
GROUP BY make
ORDER BY model_count DESC
LIMIT 10;

\echo ''

-- ================================================================
-- QUERY 2: Body Style Analysis
-- Business Question: "What's our product mix by body style?"
-- ================================================================

\echo '🏎️  QUERY 2: Production Mix by Body Style'
\echo '------------------------------------------------------------'

SELECT 
    body_style,
    COUNT(*) as model_count,
    ROUND(COUNT(*)::DECIMAL / (SELECT COUNT(*) FROM dim_vehicle) * 100, 1) as percent,
    ROUND(AVG(msrp), 0) as avg_price,
    ROUND(AVG(engine_cylinders), 1) as avg_cylinders
FROM dim_vehicle
WHERE msrp IS NOT NULL
GROUP BY body_style
ORDER BY model_count DESC;

\echo ''

-- ================================================================
-- QUERY 3: EV Transition Tracking
-- Business Question: "How is electrification progressing?"
-- ================================================================

\echo '⚡ QUERY 3: Fuel Type Distribution'
\echo '------------------------------------------------------------'

SELECT 
    fuel_type,
    COUNT(*) as vehicle_count,
    ROUND(COUNT(*)::DECIMAL / (SELECT COUNT(*) FROM dim_vehicle) * 100, 2) as percent_of_fleet,
    ROUND(AVG(msrp), 0) as avg_price
FROM dim_vehicle
GROUP BY fuel_type
ORDER BY vehicle_count DESC;

\echo ''

-- ================================================================
-- QUERY 4: Equipment Reliability
-- Business Question: "Which equipment types are most reliable?"
-- ================================================================

\echo '🔧 QUERY 4: Equipment Reliability by Type'
\echo '------------------------------------------------------------'

SELECT 
    equipment_type,
    COUNT(*) as equipment_count,
    ROUND(AVG(failure_rate_percent), 2) as avg_failure_rate,
    ROUND(MIN(failure_rate_percent), 2) as min_failure_rate,
    ROUND(MAX(failure_rate_percent), 2) as max_failure_rate,
    COUNT(*) FILTER (WHERE failure_rate_percent > 5) as high_risk_count
FROM dim_equipment
GROUP BY equipment_type
ORDER BY avg_failure_rate DESC;

\echo ''

-- ================================================================
-- QUERY 5: Maintenance Priority
-- Business Question: "Which equipment needs attention urgently?"
-- ================================================================

\echo '⚠️  QUERY 5: Top 10 High-Risk Equipment'
\echo '------------------------------------------------------------'

SELECT 
    equipment_id,
    equipment_type,
    ROUND(failure_rate_percent, 2) as failure_rate,
    ROUND(air_temp_avg, 1) as avg_temp_k,
    ROUND(tool_wear_avg, 1) as avg_tool_wear,
    CASE 
        WHEN failure_rate_percent > 10 THEN 'URGENT'
        WHEN failure_rate_percent > 5 THEN 'HIGH'
        ELSE 'MEDIUM'
    END as priority
FROM dim_equipment
WHERE failure_rate_percent > 5
ORDER BY failure_rate_percent DESC
LIMIT 10;

\echo ''

-- ================================================================
-- QUERY 6: Quality Cost Analysis
-- Business Question: "Where should we invest in quality improvements?"
-- ================================================================

\echo '💰 QUERY 6: Quality Cost by Defect Category'
\echo '------------------------------------------------------------'

SELECT 
    defect_category,
    COUNT(*) as defect_types,
    ROUND(AVG(avg_repair_cost), 2) as avg_repair_cost,
    ROUND(SUM(avg_repair_cost), 2) as total_potential_cost,
    ROUND(AVG(avg_repair_time_min), 1) as avg_repair_time_min,
    COUNT(*) FILTER (WHERE is_safety_critical = TRUE) as safety_critical_count
FROM dim_quality_defect
GROUP BY defect_category
ORDER BY total_potential_cost DESC;

\echo ''

-- ================================================================
-- QUERY 7: Safety-Critical Defects
-- Business Question: "What are our recall risks?"
-- ================================================================

\echo '🚨 QUERY 7: Safety-Critical Defects by Severity'
\echo '------------------------------------------------------------'

SELECT 
    severity_level,
    defect_category,
    COUNT(*) as defect_count,
    ROUND(AVG(avg_repair_cost), 0) as avg_cost,
    STRING_AGG(defect_code, ', ' ORDER BY avg_repair_cost DESC) as defect_codes
FROM dim_quality_defect
WHERE is_safety_critical = TRUE
GROUP BY severity_level, defect_category
ORDER BY severity_level DESC, avg_cost DESC;

\echo ''

-- ================================================================
-- QUERY 8: Production Capacity
-- Business Question: "What's our theoretical production capacity?"
-- ================================================================

\echo '🏭 QUERY 8: Production Capacity Analysis'
\echo '------------------------------------------------------------'

SELECT 
    line_name,
    line_type,
    capacity_per_shift,
    capacity_per_shift * 3 as capacity_per_day,
    capacity_per_shift * 3 * 365 as capacity_per_year,
    location,
    is_active
FROM dim_production_line
ORDER BY capacity_per_shift DESC;

\echo ''
\echo 'Total Plant Capacity:'

SELECT 
    SUM(capacity_per_shift) as total_per_shift,
    SUM(capacity_per_shift) * 3 as total_per_day,
    SUM(capacity_per_shift) * 3 * 22 as total_per_month,
    SUM(capacity_per_shift) * 3 * 365 as total_per_year
FROM dim_production_line
WHERE is_active = TRUE;

\echo ''

-- ================================================================
-- QUERY 9: Calendar Intelligence
-- Business Question: "How many production days this quarter?"
-- ================================================================

\echo '📅 QUERY 9: Q1 2025 Production Days'
\echo '------------------------------------------------------------'

SELECT 
    quarter,
    year,
    COUNT(*) as total_days,
    COUNT(*) FILTER (WHERE is_weekend = FALSE) as weekdays,
    COUNT(*) FILTER (WHERE is_weekend = TRUE) as weekend_days,
    COUNT(*) FILTER (WHERE is_holiday = TRUE) as holidays,
    COUNT(*) FILTER (WHERE is_weekend = FALSE AND is_holiday = FALSE) as production_days
FROM dim_date
WHERE year = 2025 AND quarter = 1
GROUP BY quarter, year;

\echo ''

-- ================================================================
-- QUERY 10: Shift Economics
-- Business Question: "What's the cost difference between shifts?"
-- ================================================================

\echo '⏰ QUERY 10: Shift Cost Analysis'
\echo '------------------------------------------------------------'

SELECT 
    shift_name,
    shift_code,
    start_time,
    end_time,
    shift_premium_percent,
    CASE 
        WHEN shift_premium_percent = 0 THEN 'Standard pay'
        ELSE '+' || shift_premium_percent::TEXT || '% premium'
    END as pay_rate,
    EXTRACT(HOUR FROM (end_time - start_time)) as duration_hours
FROM dim_shift
ORDER BY shift_key;

\echo ''
\echo '💡 Business Insight:'
\echo '   Night shift costs +15% more in labor but allows 24/7 production.'
\echo '   Cost-benefit analysis needed based on demand vs. labor premium.'

\echo ''
\echo '============================================================'
\echo 'BUSINESS QUERIES COMPLETE'
\echo '============================================================'
\echo ''
