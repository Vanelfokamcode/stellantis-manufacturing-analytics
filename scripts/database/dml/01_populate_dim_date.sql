-- ================================================================
-- STELLANTIS MANUFACTURING ANALYTICS
-- Script: Populate dim_date
-- Author: Vanel
-- Date: January 2025
-- Description: Generate 10 years of calendar data (2020-2030)
-- Business: Enable time-based analytics (YoY, QoQ trends)
-- ================================================================

SET search_path TO warehouse;

-- Delete existing data (if re-running)
TRUNCATE TABLE dim_date;

-- Generate dates using PostgreSQL generate_series
INSERT INTO dim_date (
    date,
    day_of_week,
    day_num,
    week_number,
    month_number,
    month_name,
    quarter,
    year,
    is_weekend,
    is_holiday
)
SELECT 
    date_value::DATE AS date,
    TO_CHAR(date_value, 'Day') AS day_of_week,
    EXTRACT(DAY FROM date_value)::INTEGER AS day_num,
    EXTRACT(WEEK FROM date_value)::INTEGER AS week_number,
    EXTRACT(MONTH FROM date_value)::INTEGER AS month_number,
    TO_CHAR(date_value, 'Month') AS month_name,
    EXTRACT(QUARTER FROM date_value)::INTEGER AS quarter,
    EXTRACT(YEAR FROM date_value)::INTEGER AS year,
    CASE 
        WHEN EXTRACT(DOW FROM date_value) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    FALSE AS is_holiday  -- Default FALSE, manual update for actual holidays
FROM generate_series(
    '2020-01-01'::DATE,
    '2030-12-31'::DATE,
    '1 day'::INTERVAL
) AS date_value;

-- Verify
SELECT 
    COUNT(*) AS total_rows,
    MIN(date) AS first_date,
    MAX(date) AS last_date,
    COUNT(*) FILTER (WHERE is_weekend = TRUE) AS weekend_days,
    COUNT(DISTINCT year) AS years_covered
FROM dim_date;

-- Expected output:
--  total_rows | first_date | last_date  | weekend_days | years_covered
-- ------------+------------+------------+--------------+---------------
--    4018     | 2020-01-01 | 2030-12-31 |     ~1150    |      11

-- Sample data
SELECT * FROM dim_date 
WHERE date BETWEEN '2024-01-01' AND '2024-01-07'
ORDER BY date;
