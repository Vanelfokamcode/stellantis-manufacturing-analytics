-- ================================================================
-- STELLANTIS MANUFACTURING ANALYTICS
-- Script: Populate dim_production_line
-- Author: Vanel
-- Date: January 2025
-- Description: Insert 5 production lines
-- Business: Track OEE by production line
-- ================================================================

SET search_path TO warehouse;

-- Delete existing data
TRUNCATE TABLE dim_production_line CASCADE;

-- Insert 5 production lines
INSERT INTO dim_production_line (
    line_name,
    capacity_per_shift,
    location,
    line_type,
    install_date,
    is_active
) VALUES
    ('Line_1', 200, 'Building A - North Wing', 'Final Assembly', '2015-03-15', TRUE),
    ('Line_2', 180, 'Building A - South Wing', 'Final Assembly', '2016-07-22', TRUE),
    ('Line_3', 220, 'Building B - East Wing',  'Body Welding',    '2018-01-10', TRUE),
    ('Line_4', 150, 'Building B - West Wing',  'Paint Shop',      '2019-05-30', TRUE),
    ('Line_5', 190, 'Building C - Main Floor', 'Final Assembly', '2020-11-12', TRUE);

-- Verify
SELECT 
    line_key,
    line_name,
    capacity_per_shift,
    location,
    line_type,
    is_active
FROM dim_production_line 
ORDER BY line_key;

-- Expected output:
--  line_key | line_name | capacity_per_shift |         location          |   line_type
-- ----------+-----------+--------------------+---------------------------+----------------
--     1     | Line_1    |        200         | Building A - North Wing   | Final Assembly
--     2     | Line_2    |        180         | Building A - South Wing   | Final Assembly
--     3     | Line_3    |        220         | Building B - East Wing    | Body Welding
--     4     | Line_4    |        150         | Building B - West Wing    | Paint Shop
--     5     | Line_5    |        190         | Building C - Main Floor   | Final Assembly

-- Total capacity per shift
SELECT SUM(capacity_per_shift) AS total_capacity_per_shift
FROM dim_production_line
WHERE is_active = TRUE;

-- Expected: 940 units/shift capacity
