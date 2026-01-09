-- analyses/data_quality_summary.sql
-- Data Quality Dashboard - Summary of all quality metrics

-- Production Data Quality
WITH production_quality AS (
    SELECT
        'Production Data' as data_source,
        COUNT(*) as total_records,
        COUNT(*) FILTER (WHERE oee_percent < 0 OR oee_percent > 100) as quality_issues,
        COUNT(*) FILTER (WHERE defects > units_produced) as logic_errors,
        COUNT(*) FILTER (WHERE date > CURRENT_DATE) as future_dates,
        MIN(date) as earliest_date,
        MAX(date) as latest_date
    FROM {{ ref('int_production_enriched') }}
),

-- Maintenance Data Quality
maintenance_quality AS (
    SELECT
        'Maintenance Data' as data_source,
        COUNT(*) as total_records,
        COUNT(*) FILTER (WHERE failure_risk_score < 0 OR failure_risk_score > 100) as quality_issues,
        COUNT(*) FILTER (WHERE air_temperature_c < 10 OR air_temperature_c > 45) as logic_errors,
        COUNT(*) FILTER (WHERE machine_failure = 1) as total_failures,
        MIN(air_temperature_c) as min_temp,
        MAX(air_temperature_c) as max_temp
    FROM {{ ref('int_maintenance_features') }}
),

-- Quality Sensor Data
quality_sensors AS (
    SELECT
        'Quality Sensors' as data_source,
        COUNT(*) as total_records,
        COUNT(*) FILTER (WHERE is_anomaly = true) as quality_issues,
        COUNT(*) FILTER (WHERE quality_status = 'FAIL') as logic_errors,
        AVG(avg_sensor_reading) as avg_reading,
        MIN(avg_sensor_reading) as min_reading,
        MAX(avg_sensor_reading) as max_reading
    FROM {{ ref('int_quality_aggregated') }}
)

-- Combined Summary
SELECT 
    data_source,
    total_records,
    quality_issues,
    logic_errors,
    -- Calculate data quality score (0-100)
    100 - ((quality_issues + logic_errors)::FLOAT / NULLIF(total_records, 0) * 100) as data_quality_score_pct,
    CURRENT_TIMESTAMP as report_generated_at
FROM production_quality

UNION ALL

SELECT 
    data_source,
    total_records,
    quality_issues,
    logic_errors,
    100 - ((quality_issues + logic_errors)::FLOAT / NULLIF(total_records, 0) * 100) as data_quality_score_pct,
    CURRENT_TIMESTAMP
FROM maintenance_quality

UNION ALL

SELECT 
    data_source,
    total_records,
    quality_issues,
    logic_errors,
    100 - ((quality_issues + logic_errors)::FLOAT / NULLIF(total_records, 0) * 100) as data_quality_score_pct,
    CURRENT_TIMESTAMP
FROM quality_sensors

ORDER BY data_quality_score_pct ASC
