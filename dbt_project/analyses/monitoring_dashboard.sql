-- analyses/monitoring_dashboard.sql
-- Monitoring Dashboard - Real-time pipeline health

-- Section 1: Data Freshness
WITH freshness AS (
    SELECT 
        'Executive KPIs' as dataset,
        MAX(date) as last_update,
        NOW()::date - MAX(date) as days_old,
        CASE 
            WHEN NOW()::date - MAX(date) < 2 THEN '✅ Fresh'
            ELSE '⚠️ Stale'
        END as status
    FROM {{ ref('mart_executive_kpis') }}
),

-- Section 2: Row Counts
row_counts AS (
    SELECT 
        'Staging' as layer,
        COUNT(*) as total_rows
    FROM {{ ref('stg_production') }}
    
    UNION ALL
    
    SELECT 
        'Marts',
        (SELECT COUNT(*) FROM {{ ref('mart_executive_kpis') }}) +
        (SELECT COUNT(*) FROM {{ ref('mart_line_performance') }}) +
        (SELECT COUNT(*) FROM {{ ref('mart_maintenance_overview') }})
),

-- Section 3: Data Quality Scores
quality_scores AS (
    SELECT
        'OEE Validity' as check_name,
        COUNT(*) FILTER (WHERE oee_percent BETWEEN 0 AND 100)::FLOAT / 
            NULLIF(COUNT(*), 0) * 100 as quality_pct
    FROM {{ ref('int_production_enriched') }}
    
    UNION ALL
    
    SELECT
        'No Nulls in Critical Fields',
        COUNT(*) FILTER (WHERE oee_percent IS NOT NULL AND units_produced IS NOT NULL)::FLOAT / 
            NULLIF(COUNT(*), 0) * 100
    FROM {{ ref('int_production_enriched') }}
)

-- Final Report
SELECT 
    '🏥 PIPELINE HEALTH DASHBOARD' as title,
    NOW() as report_time
    
UNION ALL

SELECT 
    '═══════════════════════════════════════',
    NULL

UNION ALL

SELECT 
    '📊 DATA FRESHNESS:',
    NULL
FROM freshness

UNION ALL

SELECT
    CONCAT('   ', dataset, ': ', last_update, ' (', days_old, ' days old) - ', status),
    NULL
FROM freshness

UNION ALL

SELECT 
    '',
    NULL

UNION ALL

SELECT
    '📈 ROW COUNTS:',
    NULL

UNION ALL

SELECT
    CONCAT('   ', layer, ': ', total_rows, ' rows'),
    NULL
FROM row_counts

UNION ALL

SELECT 
    '',
    NULL

UNION ALL

SELECT
    '✅ QUALITY SCORES:',
    NULL

UNION ALL

SELECT
    CONCAT('   ', check_name, ': ', ROUND(quality_pct, 2), '%'),
    NULL
FROM quality_scores

UNION ALL

SELECT 
    '═══════════════════════════════════════',
    NULL
