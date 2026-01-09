-- ================================================================
-- STELLANTIS MANUFACTURING ANALYTICS
-- Script: Advanced Business Analytics Queries
-- Author: Vanel
-- Date: January 2025
-- Description: 15+ queries demonstrating warehouse capabilities
-- Business: Real insights for executive decision-making
-- ================================================================

\set QUIET on
\pset border 2
\pset format wrapped

\echo ''
\echo '============================================================'
\echo 'STELLANTIS MANUFACTURING ANALYTICS'
\echo 'Advanced Business Queries'
\echo '============================================================'
\echo ''

SET search_path TO warehouse;

-- ================================================================
-- QUERY 1: OEE Performance Ranking by Production Line
-- Business: "Which lines are our stars vs laggards?"
-- ================================================================

\echo '📊 QUERY 1: Production Line Performance Ranking'
\echo '------------------------------------------------------------'

SELECT 
    pl.line_name,
    COUNT(*) as production_runs,
    ROUND(AVG(f.oee_percent), 2) as avg_oee,
    SUM(f.units_produced) as total_units,
    SUM(f.defects) as total_defects,
    ROUND(SUM(f.defects)::DECIMAL / NULLIF(SUM(f.units_produced), 0) * 100, 2) as defect_rate_pct,
    RANK() OVER (ORDER BY AVG(f.oee_percent) DESC) as oee_rank
FROM fact_production_metrics f
JOIN dim_production_line pl ON f.line_key = pl.line_key
GROUP BY pl.line_name
ORDER BY avg_oee DESC;

\echo ''

-- ================================================================
-- QUERY 2: Shift Performance Deep Dive
-- Business: "What's the true cost of night shift?"
-- ================================================================

\echo '⏰ QUERY 2: Shift Performance Analysis with Costs'
\echo '------------------------------------------------------------'

WITH shift_performance AS (
    SELECT 
        s.shift_name,
        s.shift_premium_percent,
        COUNT(*) as runs,
        ROUND(AVG(f.oee_percent), 2) as avg_oee,
        SUM(f.units_produced) as total_units,
        ROUND(AVG(f.downtime_minutes), 1) as avg_downtime,
        ROUND(AVG(f.defects::DECIMAL / NULLIF(f.units_produced, 0) * 100), 2) as avg_defect_rate
    FROM fact_production_metrics f
    JOIN dim_shift s ON f.shift_key = s.shift_key
    GROUP BY s.shift_name, s.shift_premium_percent
)
SELECT 
    shift_name,
    runs,
    avg_oee,
    total_units,
    avg_downtime,
    avg_defect_rate,
    shift_premium_percent,
    CASE 
        WHEN avg_oee < 75 AND shift_premium_percent > 0 THEN 'HIGH COST / LOW PERFORMANCE ⚠️'
        WHEN avg_oee >= 80 THEN 'EXCELLENT ✅'
        WHEN avg_oee >= 75 THEN 'GOOD'
        ELSE 'NEEDS IMPROVEMENT'
    END as performance_rating
FROM shift_performance
ORDER BY avg_oee DESC;

\echo ''

-- ================================================================
-- QUERY 3: Daily OEE Trends (7-Day Moving Average)
-- Business: "Is performance improving or declining?"
-- ================================================================

\echo '📈 QUERY 3: OEE Trends - 7-Day Moving Average'
\echo '------------------------------------------------------------'

SELECT 
    d.date,
    ROUND(AVG(f.oee_percent), 2) as daily_avg_oee,
    ROUND(AVG(AVG(f.oee_percent)) OVER (
        ORDER BY d.date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) as ma_7day_oee,
    SUM(f.units_produced) as daily_units
FROM fact_production_metrics f
JOIN dim_date d ON f.date_key = d.date
GROUP BY d.date
ORDER BY d.date DESC
LIMIT 30;

\echo ''

-- ================================================================
-- QUERY 4: Production vs Target - Variance Analysis
-- Business: "Are we meeting our targets?"
-- ================================================================

\echo '🎯 QUERY 4: Target Achievement Analysis'
\echo '------------------------------------------------------------'

SELECT 
    pl.line_name,
    SUM(f.units_produced) as actual_production,
    SUM(f.units_target) as target_production,
    SUM(f.units_produced) - SUM(f.units_target) as variance,
    ROUND(
        (SUM(f.units_produced)::DECIMAL / NULLIF(SUM(f.units_target), 0) - 1) * 100, 
        2
    ) as variance_pct,
    CASE 
        WHEN SUM(f.units_produced) >= SUM(f.units_target) THEN '✅ Target Met'
        WHEN SUM(f.units_produced) >= SUM(f.units_target) * 0.95 THEN '⚠️ Close (>95%)'
        ELSE '❌ Below Target'
    END as status
FROM fact_production_metrics f
JOIN dim_production_line pl ON f.line_key = pl.line_key
GROUP BY pl.line_name
ORDER BY variance_pct DESC;

\echo ''

-- ================================================================
-- QUERY 5: Top 10 Best & Worst Production Days
-- Business: "Learn from best days, fix worst days"
-- ================================================================

\echo '🏆 QUERY 5: Best Production Days (Top 10)'
\echo '------------------------------------------------------------'

SELECT 
    d.date,
    d.day_of_week,
    ROUND(AVG(f.oee_percent), 2) as avg_oee,
    SUM(f.units_produced) as total_units,
    ROUND(AVG(f.downtime_minutes), 1) as avg_downtime
FROM fact_production_metrics f
JOIN dim_date d ON f.date_key = d.date
GROUP BY d.date, d.day_of_week
ORDER BY avg_oee DESC
LIMIT 10;

\echo ''
\echo '⚠️  QUERY 5b: Worst Production Days (Bottom 10)'
\echo '------------------------------------------------------------'

SELECT 
    d.date,
    d.day_of_week,
    ROUND(AVG(f.oee_percent), 2) as avg_oee,
    SUM(f.units_produced) as total_units,
    ROUND(AVG(f.downtime_minutes), 1) as avg_downtime
FROM fact_production_metrics f
JOIN dim_date d ON f.date_key = d.date
GROUP BY d.date, d.day_of_week
ORDER BY avg_oee ASC
LIMIT 10;

\echo ''

-- ================================================================
-- QUERY 6: Day of Week Performance Pattern
-- Business: "Do Mondays really outperform?"
-- ================================================================

\echo '📅 QUERY 6: Day of Week Performance Pattern'
\echo '------------------------------------------------------------'

SELECT 
    d.day_of_week,
    COUNT(*) as production_days,
    ROUND(AVG(f.oee_percent), 2) as avg_oee,
    SUM(f.units_produced) as total_units,
    ROUND(AVG(f.downtime_minutes), 1) as avg_downtime,
    RANK() OVER (ORDER BY AVG(f.oee_percent) DESC) as performance_rank
FROM fact_production_metrics f
JOIN dim_date d ON f.date_key = d.date
GROUP BY d.day_of_week
ORDER BY performance_rank;

\echo ''

-- ================================================================
-- QUERY 7: Line + Shift Combination Analysis
-- Business: "Which line/shift combos are problematic?"
-- ================================================================

\echo '🔍 QUERY 7: Line × Shift Performance Matrix'
\echo '------------------------------------------------------------'

SELECT 
    pl.line_name,
    s.shift_name,
    COUNT(*) as runs,
    ROUND(AVG(f.oee_percent), 2) as avg_oee,
    SUM(f.defects) as total_defects,
    CASE 
        WHEN AVG(f.oee_percent) >= 80 THEN '🟢 Excellent'
        WHEN AVG(f.oee_percent) >= 75 THEN '🟡 Good'
        WHEN AVG(f.oee_percent) >= 70 THEN '🟠 Acceptable'
        ELSE '🔴 Needs Attention'
    END as status
FROM fact_production_metrics f
JOIN dim_production_line pl ON f.line_key = pl.line_key
JOIN dim_shift s ON f.shift_key = s.shift_key
GROUP BY pl.line_name, s.shift_name
ORDER BY avg_oee ASC
LIMIT 15;

\echo ''

-- ================================================================
-- QUERY 8: Capacity Utilization Analysis
-- Business: "Are we using our capacity efficiently?"
-- ================================================================

\echo '⚙️  QUERY 8: Production Capacity Utilization'
\echo '------------------------------------------------------------'

SELECT 
    pl.line_name,
    pl.capacity_per_shift,
    COUNT(*) as shifts_run,
    SUM(f.units_produced) as actual_production,
    COUNT(*) * pl.capacity_per_shift as theoretical_capacity,
    ROUND(
        SUM(f.units_produced)::DECIMAL / 
        NULLIF(COUNT(*) * pl.capacity_per_shift, 0) * 100, 
        2
    ) as capacity_utilization_pct,
    CASE 
        WHEN SUM(f.units_produced)::DECIMAL / NULLIF(COUNT(*) * pl.capacity_per_shift, 0) >= 0.85 THEN '✅ Optimal'
        WHEN SUM(f.units_produced)::DECIMAL / NULLIF(COUNT(*) * pl.capacity_per_shift, 0) >= 0.70 THEN '⚠️ Acceptable'
        ELSE '❌ Underutilized'
    END as utilization_status
FROM fact_production_metrics f
JOIN dim_production_line pl ON f.line_key = pl.line_key
GROUP BY pl.line_name, pl.capacity_per_shift
ORDER BY capacity_utilization_pct DESC;

\echo ''

-- ================================================================
-- QUERY 9: Defect Rate Trends Over Time
-- Business: "Is quality improving?"
-- ================================================================

\echo '🔬 QUERY 9: Quality Trends - Defect Rate Over Time'
\echo '------------------------------------------------------------'

SELECT 
    d.date,
    SUM(f.units_produced) as units,
    SUM(f.defects) as defects,
    ROUND(SUM(f.defects)::DECIMAL / NULLIF(SUM(f.units_produced), 0) * 100, 2) as defect_rate_pct,
    ROUND(AVG(SUM(f.defects)::DECIMAL / NULLIF(SUM(f.units_produced), 0) * 100) OVER (
        ORDER BY d.date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) as ma_7day_defect_rate
FROM fact_production_metrics f
JOIN dim_date d ON f.date_key = d.date
GROUP BY d.date
ORDER BY d.date DESC
LIMIT 30;

\echo ''

-- ================================================================
-- QUERY 10: Downtime Impact Analysis
-- Business: "What's downtime costing us?"
-- ================================================================

\echo '⏱️  QUERY 10: Downtime Cost Analysis'
\echo '------------------------------------------------------------'

WITH downtime_impact AS (
    SELECT 
        pl.line_name,
        SUM(f.downtime_minutes) as total_downtime_min,
        ROUND(SUM(f.downtime_minutes) / 60.0, 1) as total_downtime_hours,
        -- Assume $500/hour opportunity cost (lost production value)
        ROUND(SUM(f.downtime_minutes) / 60.0 * 500, 0) as estimated_cost_usd
    FROM fact_production_metrics f
    JOIN dim_production_line pl ON f.line_key = pl.line_key
    GROUP BY pl.line_name
)
SELECT 
    line_name,
    total_downtime_hours,
    estimated_cost_usd,
    RANK() OVER (ORDER BY estimated_cost_usd DESC) as cost_rank
FROM downtime_impact
ORDER BY estimated_cost_usd DESC;

\echo ''

-- ================================================================
-- QUERY 11: Production Consistency Analysis (Coefficient of Variation)
-- Business: "Which lines are most predictable?"
-- ================================================================

\echo '📊 QUERY 11: Production Consistency (Variability Analysis)'
\echo '------------------------------------------------------------'

SELECT 
    pl.line_name,
    COUNT(*) as production_runs,
    ROUND(AVG(f.units_produced), 0) as avg_units,
    ROUND(STDDEV(f.units_produced), 0) as stddev_units,
    ROUND(
        STDDEV(f.units_produced) / NULLIF(AVG(f.units_produced), 0) * 100, 
        2
    ) as coefficient_of_variation_pct,
    CASE 
        WHEN STDDEV(f.units_produced) / NULLIF(AVG(f.units_produced), 0) <= 0.10 THEN '✅ Highly Consistent'
        WHEN STDDEV(f.units_produced) / NULLIF(AVG(f.units_produced), 0) <= 0.20 THEN '🟡 Moderately Consistent'
        ELSE '⚠️ High Variability'
    END as consistency_rating
FROM fact_production_metrics f
JOIN dim_production_line pl ON f.line_key = pl.line_key
GROUP BY pl.line_name
HAVING COUNT(*) >= 10  -- At least 10 runs for statistical relevance
ORDER BY coefficient_of_variation_pct;

\echo ''

-- ================================================================
-- QUERY 12: Weekend vs Weekday Performance
-- Business: "Does weekend maintenance help?"
-- ================================================================

\echo '🗓️  QUERY 12: Weekend Effect Analysis'
\echo '------------------------------------------------------------'

SELECT 
    CASE 
        WHEN d.is_weekend THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    COUNT(*) as production_days,
    ROUND(AVG(f.oee_percent), 2) as avg_oee,
    SUM(f.units_produced) as total_units,
    ROUND(AVG(f.downtime_minutes), 1) as avg_downtime
FROM fact_production_metrics f
JOIN dim_date d ON f.date_key = d.date
GROUP BY d.is_weekend
ORDER BY avg_oee DESC;

\echo ''

-- ================================================================
-- QUERY 13: Running Total Production (Cumulative)
-- Business: "Track progress toward monthly/quarterly goals"
-- ================================================================

\echo '📈 QUERY 13: Cumulative Production (Running Total)'
\echo '------------------------------------------------------------'

SELECT 
    d.date,
    SUM(f.units_produced) as daily_production,
    SUM(SUM(f.units_produced)) OVER (
        ORDER BY d.date 
        ROWS UNBOUNDED PRECEDING
    ) as cumulative_production
FROM fact_production_metrics f
JOIN dim_date d ON f.date_key = d.date
GROUP BY d.date
ORDER BY d.date DESC
LIMIT 20;

\echo ''

-- ================================================================
-- QUERY 14: Top Performers - Lines with Best OEE Improvement
-- Business: "Who's improving the most?"
-- ================================================================

\echo '🚀 QUERY 14: Performance Improvement Analysis'
\echo '------------------------------------------------------------'

WITH period_performance AS (
    SELECT 
        pl.line_name,
        d.date,
        AVG(f.oee_percent) as avg_oee,
        CASE 
            WHEN d.date <= (SELECT MIN(date) + INTERVAL '15 days' FROM dim_date WHERE date IN (SELECT date_key FROM fact_production_metrics))
            THEN 'First Period'
            WHEN d.date >= (SELECT MAX(date) - INTERVAL '15 days' FROM dim_date WHERE date IN (SELECT date_key FROM fact_production_metrics))
            THEN 'Last Period'
            ELSE 'Middle'
        END as period
    FROM fact_production_metrics f
    JOIN dim_production_line pl ON f.line_key = pl.line_key
    JOIN dim_date d ON f.date_key = d.date
    GROUP BY pl.line_name, d.date
)
SELECT 
    line_name,
    ROUND(AVG(CASE WHEN period = 'First Period' THEN avg_oee END), 2) as first_period_oee,
    ROUND(AVG(CASE WHEN period = 'Last Period' THEN avg_oee END), 2) as last_period_oee,
    ROUND(
        AVG(CASE WHEN period = 'Last Period' THEN avg_oee END) - 
        AVG(CASE WHEN period = 'First Period' THEN avg_oee END), 
        2
    ) as oee_improvement,
    CASE 
        WHEN AVG(CASE WHEN period = 'Last Period' THEN avg_oee END) > AVG(CASE WHEN period = 'First Period' THEN avg_oee END)
        THEN '📈 Improving'
        ELSE '📉 Declining'
    END as trend
FROM period_performance
WHERE period IN ('First Period', 'Last Period')
GROUP BY line_name
ORDER BY oee_improvement DESC NULLS LAST;

\echo ''

-- ================================================================
-- QUERY 15: Executive Summary Dashboard
-- Business: "One-page overview for CEO"
-- ================================================================

\echo '📋 QUERY 15: Executive Summary - Key Metrics'
\echo '------------------------------------------------------------'

SELECT 
    'Total Production' as metric,
    SUM(units_produced)::TEXT || ' units' as value
FROM fact_production_metrics

UNION ALL

SELECT 
    'Average OEE',
    ROUND(AVG(oee_percent), 2)::TEXT || '%'
FROM fact_production_metrics

UNION ALL

SELECT 
    'Best Performing Line',
    (SELECT line_name FROM dim_production_line pl 
     JOIN fact_production_metrics f ON pl.line_key = f.line_key 
     GROUP BY line_name 
     ORDER BY AVG(f.oee_percent) DESC LIMIT 1)
FROM (SELECT 1) dummy

UNION ALL

SELECT 
    'Best Performing Shift',
    (SELECT shift_name FROM dim_shift s 
     JOIN fact_production_metrics f ON s.shift_key = f.shift_key 
     GROUP BY shift_name 
     ORDER BY AVG(f.oee_percent) DESC LIMIT 1)
FROM (SELECT 1) dummy

UNION ALL

SELECT 
    'Total Defects',
    SUM(defects)::TEXT || ' units'
FROM fact_production_metrics

UNION ALL

SELECT 
    'Overall Defect Rate',
    ROUND(SUM(defects)::DECIMAL / NULLIF(SUM(units_produced), 0) * 100, 2)::TEXT || '%'
FROM fact_production_metrics

UNION ALL

SELECT 
    'Total Downtime',
    ROUND(SUM(downtime_minutes) / 60.0, 1)::TEXT || ' hours'
FROM fact_production_metrics

UNION ALL

SELECT 
    'Production Days',
    COUNT(DISTINCT date_key)::TEXT || ' days'
FROM fact_production_metrics;

\echo ''
\echo '============================================================'
\echo 'ANALYTICS QUERIES COMPLETE'
\echo '============================================================'
\echo ''
\echo '💡 These queries demonstrate:'
\echo '   - Multi-table joins'
\echo '   - Window functions (moving averages, rankings)'
\echo '   - Advanced aggregations'
\echo '   - Business KPIs'
\echo '   - Statistical analysis'
\echo ''
\echo '🎯 Ready for Power BI dashboards (Week 7)!'
\echo ''
