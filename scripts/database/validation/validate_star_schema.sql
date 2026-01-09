-- ================================================================
-- STELLANTIS MANUFACTURING ANALYTICS
-- Script: Validate Star Schema
-- Author: Vanel
-- Date: January 2025
-- Description: Comprehensive validation of dimension tables
-- Business: Ensure data quality before production use
-- ================================================================

\set QUIET on
\pset border 2
\pset format wrapped

\echo ''
\echo '============================================================'
\echo 'STELLANTIS MANUFACTURING ANALYTICS'
\echo 'Star Schema Validation Report'
\echo '============================================================'
\echo ''

SET search_path TO warehouse;

-- ================================================================
-- TEST 1: ROW COUNTS
-- Business: Ensure all dimensions are populated
-- ================================================================

\echo '📊 TEST 1: DIMENSION ROW COUNTS'
\echo '------------------------------------------------------------'

SELECT 
    'dim_date' as table_name,
    COUNT(*) as row_count,
    '4,018 expected (10 years)' as expected,
    CASE 
        WHEN COUNT(*) >= 3650 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END as status
FROM dim_date

UNION ALL

SELECT 
    'dim_shift',
    COUNT(*),
    '3 expected',
    CASE 
        WHEN COUNT(*) = 3 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_shift

UNION ALL

SELECT 
    'dim_production_line',
    COUNT(*),
    '5 expected',
    CASE 
        WHEN COUNT(*) = 5 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_production_line

UNION ALL

SELECT 
    'dim_vehicle',
    COUNT(*),
    '~11,914 expected',
    CASE 
        WHEN COUNT(*) >= 11000 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_vehicle

UNION ALL

SELECT 
    'dim_equipment',
    COUNT(*),
    '10,000 expected',
    CASE 
        WHEN COUNT(*) = 10000 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_equipment

UNION ALL

SELECT 
    'dim_quality_defect',
    COUNT(*),
    '56 expected',
    CASE 
        WHEN COUNT(*) = 56 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_quality_defect;

\echo ''

-- ================================================================
-- TEST 2: NULL VALUES IN CRITICAL COLUMNS
-- Business: Primary keys and critical fields must not be null
-- ================================================================

\echo '🔍 TEST 2: NULL VALUES IN CRITICAL COLUMNS'
\echo '------------------------------------------------------------'

SELECT 
    'dim_date.date' as column_name,
    COUNT(*) FILTER (WHERE date IS NULL) as null_count,
    CASE 
        WHEN COUNT(*) FILTER (WHERE date IS NULL) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END as status
FROM dim_date

UNION ALL

SELECT 
    'dim_shift.shift_name',
    COUNT(*) FILTER (WHERE shift_name IS NULL),
    CASE 
        WHEN COUNT(*) FILTER (WHERE shift_name IS NULL) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_shift

UNION ALL

SELECT 
    'dim_production_line.line_name',
    COUNT(*) FILTER (WHERE line_name IS NULL),
    CASE 
        WHEN COUNT(*) FILTER (WHERE line_name IS NULL) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_production_line

UNION ALL

SELECT 
    'dim_vehicle.make',
    COUNT(*) FILTER (WHERE make IS NULL),
    CASE 
        WHEN COUNT(*) FILTER (WHERE make IS NULL) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_vehicle

UNION ALL

SELECT 
    'dim_vehicle.model',
    COUNT(*) FILTER (WHERE model IS NULL),
    CASE 
        WHEN COUNT(*) FILTER (WHERE model IS NULL) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_vehicle

UNION ALL

SELECT 
    'dim_equipment.equipment_id',
    COUNT(*) FILTER (WHERE equipment_id IS NULL),
    CASE 
        WHEN COUNT(*) FILTER (WHERE equipment_id IS NULL) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_equipment

UNION ALL

SELECT 
    'dim_quality_defect.defect_code',
    COUNT(*) FILTER (WHERE defect_code IS NULL),
    CASE 
        WHEN COUNT(*) FILTER (WHERE defect_code IS NULL) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_quality_defect;

\echo ''

-- ================================================================
-- TEST 3: DUPLICATE CHECK
-- Business: Primary keys must be unique
-- ================================================================

\echo '🔎 TEST 3: DUPLICATE PRIMARY KEYS'
\echo '------------------------------------------------------------'

SELECT 
    'dim_date' as table_name,
    COUNT(*) - COUNT(DISTINCT date) as duplicate_count,
    CASE 
        WHEN COUNT(*) - COUNT(DISTINCT date) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END as status
FROM dim_date

UNION ALL

SELECT 
    'dim_shift',
    COUNT(*) - COUNT(DISTINCT shift_key),
    CASE 
        WHEN COUNT(*) - COUNT(DISTINCT shift_key) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_shift

UNION ALL

SELECT 
    'dim_production_line',
    COUNT(*) - COUNT(DISTINCT line_key),
    CASE 
        WHEN COUNT(*) - COUNT(DISTINCT line_key) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_production_line

UNION ALL

SELECT 
    'dim_vehicle',
    COUNT(*) - COUNT(DISTINCT vehicle_key),
    CASE 
        WHEN COUNT(*) - COUNT(DISTINCT vehicle_key) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_vehicle

UNION ALL

SELECT 
    'dim_equipment',
    COUNT(*) - COUNT(DISTINCT equipment_key),
    CASE 
        WHEN COUNT(*) - COUNT(DISTINCT equipment_key) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_equipment

UNION ALL

SELECT 
    'dim_quality_defect',
    COUNT(*) - COUNT(DISTINCT defect_key),
    CASE 
        WHEN COUNT(*) - COUNT(DISTINCT defect_key) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_quality_defect;

\echo ''

-- ================================================================
-- TEST 4: DATA RANGE VALIDATION
-- Business: Values must be within expected ranges
-- ================================================================

\echo '📏 TEST 4: DATA RANGE VALIDATION'
\echo '------------------------------------------------------------'

-- Date range
SELECT 
    'dim_date year range' as test,
    MIN(year)::TEXT || ' to ' || MAX(year)::TEXT as value,
    CASE 
        WHEN MIN(year) = 2020 AND MAX(year) = 2030 THEN '✅ PASS'
        ELSE '⚠️  WARNING'
    END as status
FROM dim_date

UNION ALL

-- Vehicle year range
SELECT 
    'dim_vehicle year range',
    MIN(year)::TEXT || ' to ' || MAX(year)::TEXT,
    CASE 
        WHEN MIN(year) >= 1990 AND MAX(year) <= 2030 THEN '✅ PASS'
        ELSE '⚠️  WARNING'
    END
FROM dim_vehicle

UNION ALL

-- Equipment failure rate
SELECT 
    'dim_equipment failure rate',
    'Avg: ' || ROUND(AVG(failure_rate_percent), 2)::TEXT || '%',
    CASE 
        WHEN AVG(failure_rate_percent) BETWEEN 0 AND 20 THEN '✅ PASS'
        ELSE '⚠️  WARNING'
    END
FROM dim_equipment

UNION ALL

-- Quality defect severity
SELECT 
    'dim_quality_defect severity',
    'Range: ' || MIN(severity_level)::TEXT || ' to ' || MAX(severity_level)::TEXT,
    CASE 
        WHEN MIN(severity_level) = 1 AND MAX(severity_level) = 5 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_quality_defect;

\echo ''

-- ================================================================
-- TEST 5: BUSINESS RULE VALIDATION
-- Business: Data must follow business logic
-- ================================================================

\echo '💼 TEST 5: BUSINESS RULE VALIDATION'
\echo '------------------------------------------------------------'

-- Weekend days should be ~29% of total
SELECT 
    'Weekend proportion' as test,
    ROUND(COUNT(*) FILTER (WHERE is_weekend = TRUE)::DECIMAL / COUNT(*) * 100, 1)::TEXT || '%' as value,
    CASE 
        WHEN COUNT(*) FILTER (WHERE is_weekend = TRUE)::DECIMAL / COUNT(*) BETWEEN 0.25 AND 0.32 THEN '✅ PASS'
        ELSE '⚠️  WARNING'
    END as status
FROM dim_date

UNION ALL

-- Night shift should have premium
SELECT 
    'Night shift premium',
    shift_premium_percent::TEXT || '%',
    CASE 
        WHEN shift_premium_percent > 0 THEN '✅ PASS'
        ELSE '⚠️  WARNING'
    END
FROM dim_shift
WHERE shift_name = 'Night'

UNION ALL

-- All production lines should be active
SELECT 
    'Active production lines',
    COUNT(*)::TEXT || ' / ' || (SELECT COUNT(*) FROM dim_production_line)::TEXT,
    CASE 
        WHEN COUNT(*) = (SELECT COUNT(*) FROM dim_production_line) THEN '✅ PASS'
        ELSE '⚠️  WARNING'
    END
FROM dim_production_line
WHERE is_active = TRUE

UNION ALL

-- Safety-critical defects should exist
SELECT 
    'Safety-critical defects',
    COUNT(*)::TEXT || ' defects',
    CASE 
        WHEN COUNT(*) > 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END
FROM dim_quality_defect
WHERE is_safety_critical = TRUE;

\echo ''

-- ================================================================
-- SUMMARY STATISTICS
-- ================================================================

\echo '📊 SUMMARY STATISTICS'
\echo '------------------------------------------------------------'

SELECT 
    'Total dimension rows' as metric,
    (
        (SELECT COUNT(*) FROM dim_date) +
        (SELECT COUNT(*) FROM dim_shift) +
        (SELECT COUNT(*) FROM dim_production_line) +
        (SELECT COUNT(*) FROM dim_vehicle) +
        (SELECT COUNT(*) FROM dim_equipment) +
        (SELECT COUNT(*) FROM dim_quality_defect)
    )::TEXT as value
    
UNION ALL

SELECT 
    'Unique vehicle makes',
    COUNT(DISTINCT make)::TEXT
FROM dim_vehicle

UNION ALL

SELECT 
    'Unique equipment types',
    COUNT(DISTINCT equipment_type)::TEXT
FROM dim_equipment

UNION ALL

SELECT 
    'Unique defect categories',
    COUNT(DISTINCT defect_category)::TEXT
FROM dim_quality_defect

UNION ALL

SELECT 
    'Date range coverage',
    (MAX(date) - MIN(date))::TEXT || ' days'
FROM dim_date;

\echo ''
\echo '============================================================'
\echo 'VALIDATION COMPLETE'
\echo '============================================================'
\echo ''
