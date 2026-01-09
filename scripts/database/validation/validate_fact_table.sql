-- ================================================================
-- STELLANTIS MANUFACTURING ANALYTICS
-- Script: Validate Fact Table Data Quality
-- Author: Vanel
-- Date: January 2025
-- Description: Comprehensive data quality checks on fact_production_metrics
-- Business: Ensure data integrity before business use
-- ================================================================

\set QUIET on
\pset border 2
\pset format wrapped

\echo ''
\echo '============================================================'
\echo 'FACT TABLE DATA QUALITY VALIDATION'
\echo 'Stellantis Manufacturing Analytics'
\echo '============================================================'
\echo ''

SET search_path TO warehouse;

-- ================================================================
-- TEST 1: ROW COUNT VERIFICATION
-- Business: Ensure data loaded correctly
-- ================================================================

\echo '📊 TEST 1: ROW COUNT VERIFICATION'
\echo '------------------------------------------------------------'

SELECT 
    'Staging source' as source,
    COUNT(*) as row_count
FROM staging.stg_production_metrics

UNION ALL

SELECT 
    'Fact table',
    COUNT(*)
FROM fact_production_metrics

UNION ALL

SELECT 
    'Difference',
    (SELECT COUNT(*) FROM staging.stg_production_metrics) - 
    (SELECT COUNT(*) FROM fact_production_metrics);

\echo ''

-- ================================================================
-- TEST 2: REFERENTIAL INTEGRITY (No Orphan FKs)
-- Business: All foreign keys must point to valid dimensions
-- ================================================================

\echo '🔗 TEST 2: REFERENTIAL INTEGRITY CHECKS'
\echo '------------------------------------------------------------'

-- Check orphan dates
SELECT 
    'Orphan date_key' as test,
    COUNT(*) as orphan_count,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END as status
FROM fact_production_metrics f
LEFT JOIN dim_date d ON f.date_key = d.date
WHERE d.date IS NULL

UNION ALL

-- Check orphan lines
SELECT 
    'Orphan line_key',
    COUNT(*),
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM fact_production_metrics f
LEFT JOIN dim_production_line pl ON f.line_key = pl.line_key
WHERE pl.line_key IS NULL

UNION ALL

-- Check orphan vehicles
SELECT 
    'Orphan vehicle_key',
    COUNT(*),
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM fact_production_metrics f
LEFT JOIN dim_vehicle v ON f.vehicle_key = v.vehicle_key
WHERE v.vehicle_key IS NULL

UNION ALL

-- Check orphan shifts
SELECT 
    'Orphan shift_key',
    COUNT(*),
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM fact_production_metrics f
LEFT JOIN dim_shift s ON f.shift_key = s.shift_key
WHERE s.shift_key IS NULL;

\echo ''

-- ================================================================
-- TEST 3: BUSINESS RULE VALIDATION
-- Business: Data must follow manufacturing logic
-- ================================================================

\echo '💼 TEST 3: BUSINESS RULE VALIDATION'
\echo '------------------------------------------------------------'

-- Rule 1: OEE must be between 0 and 100
SELECT 
    'OEE in valid range (0-100)' as rule,
    COUNT(*) FILTER (WHERE oee_percent NOT BETWEEN 0 AND 100) as violations,
    CASE 
        WHEN COUNT(*) FILTER (WHERE oee_percent NOT BETWEEN 0 AND 100) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END as status
FROM fact_production_metrics

UNION ALL

-- Rule 2: Defects cannot exceed production
SELECT 
    'Defects ≤ Production',
    COUNT(*) FILTER (WHERE defects > units_produced),
    CASE 
        WHEN COUNT(*) FILTER (WHERE defects > units_produced) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM fact_production_metrics

UNION ALL

-- Rule 3: Production must be positive
SELECT 
    'Production > 0',
    COUNT(*) FILTER (WHERE units_produced <= 0),
    CASE 
        WHEN COUNT(*) FILTER (WHERE units_produced <= 0) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM fact_production_metrics

UNION ALL

-- Rule 4: Downtime must be non-negative
SELECT 
    'Downtime ≥ 0',
    COUNT(*) FILTER (WHERE downtime_minutes < 0),
    CASE 
        WHEN COUNT(*) FILTER (WHERE downtime_minutes < 0) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM fact_production_metrics

UNION ALL

-- Rule 5: Cycle time must be positive
SELECT 
    'Cycle time > 0',
    COUNT(*) FILTER (WHERE cycle_time_seconds <= 0),
    CASE 
        WHEN COUNT(*) FILTER (WHERE cycle_time_seconds <= 0) = 0 THEN '✅ PASS'
        ELSE '⚠️  WARNING'
    END
FROM fact_production_metrics;

\echo ''

-- ================================================================
-- TEST 4: DATA COMPLETENESS (No NULLs in Required Fields)
-- Business: Critical fields must not be NULL
-- ================================================================

\echo '📋 TEST 4: DATA COMPLETENESS CHECKS'
\echo '------------------------------------------------------------'

SELECT 
    'NULL date_key' as field,
    COUNT(*) FILTER (WHERE date_key IS NULL) as null_count,
    CASE 
        WHEN COUNT(*) FILTER (WHERE date_key IS NULL) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END as status
FROM fact_production_metrics

UNION ALL

SELECT 
    'NULL line_key',
    COUNT(*) FILTER (WHERE line_key IS NULL),
    CASE 
        WHEN COUNT(*) FILTER (WHERE line_key IS NULL) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM fact_production_metrics

UNION ALL

SELECT 
    'NULL vehicle_key',
    COUNT(*) FILTER (WHERE vehicle_key IS NULL),
    CASE 
        WHEN COUNT(*) FILTER (WHERE vehicle_key IS NULL) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM fact_production_metrics

UNION ALL

SELECT 
    'NULL shift_key',
    COUNT(*) FILTER (WHERE shift_key IS NULL),
    CASE 
        WHEN COUNT(*) FILTER (WHERE shift_key IS NULL) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM fact_production_metrics

UNION ALL

SELECT 
    'NULL units_produced',
    COUNT(*) FILTER (WHERE units_produced IS NULL),
    CASE 
        WHEN COUNT(*) FILTER (WHERE units_produced IS NULL) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM fact_production_metrics

UNION ALL

SELECT 
    'NULL oee_percent',
    COUNT(*) FILTER (WHERE oee_percent IS NULL),
    CASE 
        WHEN COUNT(*) FILTER (WHERE oee_percent IS NULL) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM fact_production_metrics;

\echo ''

-- ================================================================
-- TEST 5: GRAIN UNIQUENESS (No Duplicates)
-- Business: Fact table grain = date + line + shift (must be unique)
-- ================================================================

\echo '🔎 TEST 5: GRAIN UNIQUENESS CHECK'
\echo '------------------------------------------------------------'

WITH duplicates AS (
    SELECT 
        date_key,
        line_key,
        shift_key,
        COUNT(*) as duplicate_count
    FROM fact_production_metrics
    GROUP BY date_key, line_key, shift_key
    HAVING COUNT(*) > 1
)
SELECT 
    'Duplicate records' as test,
    COUNT(*) as duplicate_groups,
    COALESCE(SUM(duplicate_count - 1), 0) as extra_rows,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END as status
FROM duplicates;

\echo ''

-- ================================================================
-- TEST 6: STATISTICAL SANITY CHECKS
-- Business: Values must be within reasonable manufacturing ranges
-- ================================================================

\echo '📈 TEST 6: STATISTICAL SANITY CHECKS'
\echo '------------------------------------------------------------'

SELECT 
    'OEE range' as metric,
    ROUND(MIN(oee_percent), 2)::TEXT || '% - ' || ROUND(MAX(oee_percent), 2)::TEXT || '%' as value_range,
    CASE 
        WHEN MIN(oee_percent) >= 0 AND MAX(oee_percent) <= 100 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END as status
FROM fact_production_metrics

UNION ALL

SELECT 
    'Production per shift',
    MIN(units_produced)::TEXT || ' - ' || MAX(units_produced)::TEXT || ' units',
    CASE 
        WHEN MIN(units_produced) >= 0 AND MAX(units_produced) <= 1000 THEN '✅ PASS'
        ELSE '⚠️  WARNING'
    END
FROM fact_production_metrics

UNION ALL

SELECT 
    'Defect rate',
    ROUND(MIN(defects::DECIMAL / NULLIF(units_produced, 0) * 100), 2)::TEXT || '% - ' ||
    ROUND(MAX(defects::DECIMAL / NULLIF(units_produced, 0) * 100), 2)::TEXT || '%',
    CASE 
        WHEN MAX(defects::DECIMAL / NULLIF(units_produced, 0)) <= 0.20 THEN '✅ PASS'
        ELSE '⚠️  WARNING'
    END
FROM fact_production_metrics

UNION ALL

SELECT 
    'Downtime range',
    ROUND(MIN(downtime_minutes), 1)::TEXT || ' - ' || ROUND(MAX(downtime_minutes), 1)::TEXT || ' min',
    CASE 
        WHEN MAX(downtime_minutes) <= 200 THEN '✅ PASS'
        ELSE '⚠️  WARNING'
    END
FROM fact_production_metrics;

\echo ''

-- ================================================================
-- TEST 7: DATE RANGE VALIDATION
-- Business: Dates should be within expected range
-- ================================================================

\echo '📅 TEST 7: DATE RANGE VALIDATION'
\echo '------------------------------------------------------------'

SELECT 
    'Date range' as test,
    MIN(date_key)::TEXT || ' to ' || MAX(date_key)::TEXT as date_range,
    (MAX(date_key) - MIN(date_key))::TEXT || ' days' as span,
    CASE 
        WHEN MIN(date_key) >= '2020-01-01' AND MAX(date_key) <= '2030-12-31' THEN '✅ PASS'
        ELSE '⚠️  WARNING'
    END as status
FROM fact_production_metrics;

\echo ''

-- ================================================================
-- TEST 8: COVERAGE CHECKS
-- Business: Do we have data for all expected combinations?
-- ================================================================

\echo '🎯 TEST 8: DATA COVERAGE CHECKS'
\echo '------------------------------------------------------------'

SELECT 
    'Unique dates' as dimension,
    COUNT(DISTINCT date_key) as unique_values
FROM fact_production_metrics

UNION ALL

SELECT 
    'Unique lines',
    COUNT(DISTINCT line_key)
FROM fact_production_metrics

UNION ALL

SELECT 
    'Unique shifts',
    COUNT(DISTINCT shift_key)
FROM fact_production_metrics

UNION ALL

SELECT 
    'Unique vehicles',
    COUNT(DISTINCT vehicle_key)
FROM fact_production_metrics;

\echo ''

-- ================================================================
-- SUMMARY SCORE
-- ================================================================

\echo '📊 OVERALL DATA QUALITY SCORE'
\echo '------------------------------------------------------------'

WITH quality_checks AS (
    -- Count total checks and passes
    SELECT 
        COUNT(*) as total_checks,
        SUM(CASE 
            WHEN 
                -- Orphan checks
                (SELECT COUNT(*) FROM fact_production_metrics f
                 LEFT JOIN dim_date d ON f.date_key = d.date WHERE d.date IS NULL) = 0
                AND
                (SELECT COUNT(*) FROM fact_production_metrics f
                 LEFT JOIN dim_production_line pl ON f.line_key = pl.line_key WHERE pl.line_key IS NULL) = 0
                AND
                (SELECT COUNT(*) FROM fact_production_metrics f
                 LEFT JOIN dim_vehicle v ON f.vehicle_key = v.vehicle_key WHERE v.vehicle_key IS NULL) = 0
                AND
                (SELECT COUNT(*) FROM fact_production_metrics f
                 LEFT JOIN dim_shift s ON f.shift_key = s.shift_key WHERE s.shift_key IS NULL) = 0
                -- Business rules
                AND
                (SELECT COUNT(*) FROM fact_production_metrics WHERE oee_percent NOT BETWEEN 0 AND 100) = 0
                AND
                (SELECT COUNT(*) FROM fact_production_metrics WHERE defects > units_produced) = 0
                -- No nulls
                AND
                (SELECT COUNT(*) FROM fact_production_metrics WHERE date_key IS NULL OR line_key IS NULL) = 0
                -- No duplicates
                AND
                (SELECT COUNT(*) FROM (
                    SELECT date_key, line_key, shift_key, COUNT(*) 
                    FROM fact_production_metrics 
                    GROUP BY date_key, line_key, shift_key 
                    HAVING COUNT(*) > 1
                ) dups) = 0
            THEN 1 
            ELSE 0 
        END) as checks_passed
    FROM (SELECT 1) dummy
)
SELECT 
    total_checks,
    checks_passed,
    ROUND(checks_passed::DECIMAL / total_checks * 100, 1) as quality_score_pct,
    CASE 
        WHEN checks_passed::DECIMAL / total_checks >= 0.95 THEN '✅ EXCELLENT'
        WHEN checks_passed::DECIMAL / total_checks >= 0.85 THEN '✅ GOOD'
        WHEN checks_passed::DECIMAL / total_checks >= 0.70 THEN '⚠️  ACCEPTABLE'
        ELSE '❌ NEEDS IMPROVEMENT'
    END as quality_rating
FROM quality_checks;

\echo ''
\echo '============================================================'
\echo 'VALIDATION COMPLETE'
\echo '============================================================'
\echo ''
\echo 'Next steps:'
\echo '  - Review any failed tests above'
\echo '  - Fix data quality issues if found'
\echo '  - Re-run validation after fixes'
\echo ''
