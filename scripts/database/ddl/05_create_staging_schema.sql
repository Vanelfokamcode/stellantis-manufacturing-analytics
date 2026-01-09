-- ================================================================
-- STELLANTIS MANUFACTURING ANALYTICS
-- Script: Create Staging Schema Tables
-- Author: Vanel
-- Date: January 2025
-- Description: Raw data landing zone (exact copy of CSVs)
-- Business: Preserve original data, enable ETL re-runs
-- ================================================================

SET search_path TO staging;

-- ================================================================
-- STAGING TABLE 1: Production Metrics
-- Source: production_metrics.csv
-- Purpose: Daily production data (raw from CSV)
-- ================================================================

DROP TABLE IF EXISTS stg_production_metrics CASCADE;

CREATE TABLE stg_production_metrics (
    -- CSV columns (exact match to CSV structure)
    production_date DATE,
    line_name VARCHAR(50),
    shift_name VARCHAR(20),
    vehicle_make VARCHAR(50),
    vehicle_model VARCHAR(100),
    vehicle_year INTEGER,
    
    -- Production metrics
    units_produced INTEGER,
    units_target INTEGER,
    defects INTEGER,
    downtime_minutes DECIMAL(10,2),
    cycle_time_seconds DECIMAL(10,2),
    
    -- Pre-calculated OEE (from CSV)
    oee_percent DECIMAL(5,2),
    
    -- ETL metadata
    loaded_at TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE stg_production_metrics IS 'Staging: Raw production data from CSV';
COMMENT ON COLUMN stg_production_metrics.loaded_at IS 'ETL timestamp (when loaded into staging)';

-- ================================================================
-- STAGING TABLE 2: Maintenance Data
-- Source: ai4i2020.csv
-- Purpose: Equipment operational data
-- ================================================================

DROP TABLE IF EXISTS stg_maintenance CASCADE;

CREATE TABLE stg_maintenance (
    -- Equipment identification
    udi VARCHAR(50),          -- Unique Device Identifier
    product_id VARCHAR(10),   -- Product ID
    type VARCHAR(10),         -- L/M/H
    
    -- Operating parameters
    air_temperature_k DECIMAL(10,2),
    process_temperature_k DECIMAL(10,2),
    rotational_speed_rpm DECIMAL(10,2),
    torque_nm DECIMAL(10,2),
    tool_wear_min DECIMAL(10,2),
    
    -- Failure indicators
    machine_failure INTEGER,  -- 0 or 1
    twf INTEGER,             -- Tool Wear Failure
    hdf INTEGER,             -- Heat Dissipation Failure
    pwf INTEGER,             -- Power Failure
    osf INTEGER,             -- Overstrain Failure
    rnf INTEGER,             -- Random Failure
    
    -- ETL metadata
    loaded_at TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE stg_maintenance IS 'Staging: Equipment maintenance data from ai4i2020.csv';

-- ================================================================
-- STAGING TABLE 3: Quality Defects
-- Source: uci-secom.csv
-- Purpose: Manufacturing quality sensor data
-- Note: 592 columns - we'll store key columns only for now
-- ================================================================

DROP TABLE IF EXISTS stg_quality CASCADE;

CREATE TABLE stg_quality (
    -- Identifier
    row_id SERIAL,
    
    -- Pass/Fail indicator (last column in SECOM)
    pass_fail INTEGER,  -- -1 (pass) or 1 (fail)
    
    -- Key sensor measurements (sample - we'd have 591 in production)
    -- For Day 15, we'll just create the structure
    -- Week 5 ML will use full SECOM dataset
    sensor_1 DECIMAL(10,6),
    sensor_2 DECIMAL(10,6),
    sensor_3 DECIMAL(10,6),
    -- ... (in production, we'd have sensor_1 through sensor_591)
    
    -- ETL metadata
    loaded_at TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE stg_quality IS 'Staging: Quality sensor data from SECOM (simplified for Week 3)';
COMMENT ON COLUMN stg_quality.pass_fail IS '-1 = Pass, 1 = Fail';

-- ================================================================
-- INDEXES (Optional for staging, but helpful)
-- ================================================================

-- Index on staging production date for faster lookups
CREATE INDEX idx_stg_prod_date ON stg_production_metrics(production_date);

-- Index on equipment ID
CREATE INDEX idx_stg_maint_udi ON stg_maintenance(udi);

-- ================================================================
-- VERIFY STAGING TABLES CREATED
-- ================================================================

SELECT 
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_schema = 'staging' 
       AND information_schema.columns.table_name = tables.table_name) as column_count
FROM information_schema.tables
WHERE table_schema = 'staging'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- Expected output:
--      table_name              | column_count
-- -----------------------------+--------------
--  stg_maintenance             |     14
--  stg_production_metrics      |     13
--  stg_quality                 |      6
