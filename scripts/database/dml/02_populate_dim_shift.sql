-- ================================================================
-- STELLANTIS MANUFACTURING ANALYTICS
-- Script: Populate dim_shift
-- Author: Vanel
-- Date: January 2025
-- Description: Insert 3 work shifts
-- Business: Track performance by shift (day vs night analysis)
-- ================================================================

SET search_path TO warehouse;

-- Delete existing data
TRUNCATE TABLE dim_shift CASCADE;

-- Insert 3 shifts
INSERT INTO dim_shift (
    shift_name,
    shift_code,
    start_time,
    end_time,
    shift_premium_percent
) VALUES
    ('Morning',    'M', '06:00:00', '14:00:00', 0.00),   -- No premium
    ('Afternoon',  'A', '14:00:00', '22:00:00', 0.00),   -- No premium
    ('Night',      'N', '22:00:00', '06:00:00', 15.00);  -- +15% night premium

-- Verify
SELECT * FROM dim_shift ORDER BY shift_key;

-- Expected output:
--  shift_key | shift_name | shift_code | start_time | end_time | shift_premium_percent
-- -----------+------------+------------+------------+----------+-----------------------
--     1      | Morning    | M          | 06:00:00   | 14:00:00 |          0.00
--     2      | Afternoon  | A          | 14:00:00   | 22:00:00 |          0.00
--     3      | Night      | N          | 22:00:00   | 06:00:00 |         15.00
