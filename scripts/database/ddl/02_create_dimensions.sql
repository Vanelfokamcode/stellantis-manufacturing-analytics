-- ================================================================
-- STELLANTIS MANUFACTURING ANALYTICS
-- Script: Create Dimension Tables
-- Author: Vanel
-- Date: January 2025
-- Description: Star schema dimension tables
-- Business: Provide context for fact metrics (who/what/when/where)
-- ================================================================

SET search_path TO warehouse;

-- ================================================================
-- DIMENSION 1: DATE
-- Business: Calendar intelligence for time-based analysis
-- Grain: One row per day
-- Rows: ~3,650 (10 years)
-- ================================================================

CREATE TABLE dim_date (
    -- Primary Key
    date DATE PRIMARY KEY,
    
    -- Day attributes
    day_of_week VARCHAR(10) NOT NULL,
    day_num INTEGER NOT NULL CHECK (day_num BETWEEN 1 AND 31),
    
    -- Week attributes
    week_number INTEGER NOT NULL CHECK (week_number BETWEEN 1 AND 53),
    
    -- Month attributes
    month_number INTEGER NOT NULL CHECK (month_number BETWEEN 1 AND 12),
    month_name VARCHAR(10) NOT NULL,
    
    -- Quarter/Year
    quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    year INTEGER NOT NULL CHECK (year BETWEEN 2020 AND 2030),
    
    -- Special flags
    is_weekend BOOLEAN NOT NULL DEFAULT FALSE,
    is_holiday BOOLEAN NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE dim_date IS 'Calendar dimension for time-based analytics';
COMMENT ON COLUMN dim_date.is_weekend IS 'TRUE if Saturday or Sunday';
COMMENT ON COLUMN dim_date.is_holiday IS 'TRUE if company holiday (manual update)';


-- ================================================================
-- DIMENSION 2: PRODUCTION LINE
-- Business: Manufacturing line master data
-- Grain: One row per production line
-- Rows: 5 (Line_1 to Line_5)
-- ================================================================

CREATE TABLE dim_production_line (
    -- Primary Key
    line_key SERIAL PRIMARY KEY,
    
    -- Attributes
    line_name VARCHAR(50) UNIQUE NOT NULL,
    capacity_per_shift INTEGER NOT NULL CHECK (capacity_per_shift > 0),
    location VARCHAR(100),
    line_type VARCHAR(50),
    install_date DATE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE dim_production_line IS 'Production line catalog';
COMMENT ON COLUMN dim_production_line.capacity_per_shift IS 'Maximum units producible per shift';
COMMENT ON COLUMN dim_production_line.is_active IS 'FALSE if line decommissioned';


-- ================================================================
-- DIMENSION 3: VEHICLE
-- Business: Product catalog (what we manufacture)
-- Grain: One row per vehicle model
-- Rows: ~12,000
-- ================================================================

CREATE TABLE dim_vehicle (
    -- Primary Key
    vehicle_key SERIAL PRIMARY KEY,
    
    -- Vehicle identification
    make VARCHAR(50) NOT NULL,
    model VARCHAR(100) NOT NULL,
    year INTEGER NOT NULL CHECK (year >= 1990),
    
    -- Specifications
    body_style VARCHAR(50),
    msrp DECIMAL(10,2) CHECK (msrp > 0),
    engine_cylinders INTEGER CHECK (engine_cylinders > 0),
    fuel_type VARCHAR(20),
    
    -- Status
    is_current_model BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- Unique constraint (no duplicate models)
    UNIQUE(make, model, year)
);

COMMENT ON TABLE dim_vehicle IS 'Vehicle catalog - what we produce';
COMMENT ON COLUMN dim_vehicle.msrp IS 'Manufacturer Suggested Retail Price (USD)';
COMMENT ON COLUMN dim_vehicle.is_current_model IS 'FALSE if discontinued';


-- ================================================================
-- DIMENSION 4: SHIFT
-- Business: Work shift master data
-- Grain: One row per shift
-- Rows: 3 (Morning, Afternoon, Night)
-- ================================================================

CREATE TABLE dim_shift (
    -- Primary Key
    shift_key SERIAL PRIMARY KEY,
    
    -- Attributes
    shift_name VARCHAR(20) UNIQUE NOT NULL,
    shift_code CHAR(1) UNIQUE NOT NULL,
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    shift_premium_percent DECIMAL(5,2) NOT NULL DEFAULT 0.00,
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE dim_shift IS 'Work shift schedule';
COMMENT ON COLUMN dim_shift.shift_premium_percent IS 'Additional pay % for this shift (night premium)';


-- ================================================================
-- DIMENSION 5: EQUIPMENT
-- Business: Manufacturing equipment catalog
-- Grain: One row per machine/equipment
-- Rows: ~10,000
-- ================================================================

CREATE TABLE dim_equipment (
    -- Primary Key
    equipment_key SERIAL PRIMARY KEY,
    
    -- Identification
    equipment_id VARCHAR(50) UNIQUE NOT NULL,
    equipment_type VARCHAR(50) NOT NULL,
    
    -- Operating characteristics (from maintenance data)
    air_temp_avg DECIMAL(10,2),
    rotational_speed_avg DECIMAL(10,2),
    torque_avg DECIMAL(10,2),
    tool_wear_avg DECIMAL(10,2),
    
    -- Reliability metrics
    failure_rate_percent DECIMAL(5,2) CHECK (failure_rate_percent BETWEEN 0 AND 100),
    
    -- Maintenance tracking
    last_maintenance_date DATE,
    install_date DATE,
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE dim_equipment IS 'Equipment catalog with reliability metrics';
COMMENT ON COLUMN dim_equipment.failure_rate_percent IS 'Historical failure rate (calculated from maintenance data)';


-- ================================================================
-- DIMENSION 6: QUALITY DEFECT
-- Business: Defect type classification
-- Grain: One row per defect type
-- Rows: ~100-500
-- ================================================================

CREATE TABLE dim_quality_defect (
    -- Primary Key
    defect_key SERIAL PRIMARY KEY,
    
    -- Identification
    defect_code VARCHAR(20) UNIQUE NOT NULL,
    defect_category VARCHAR(50) NOT NULL,
    defect_description TEXT,
    
    -- Severity
    severity_level INTEGER NOT NULL CHECK (severity_level BETWEEN 1 AND 5),
    
    -- Cost impact
    avg_repair_time_min DECIMAL(10,2),
    avg_repair_cost DECIMAL(10,2),
    
    -- Risk flags
    is_safety_critical BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE dim_quality_defect IS 'Defect type master data';
COMMENT ON COLUMN dim_quality_defect.severity_level IS '1=Minor, 2=Moderate, 3=Major, 4=Critical, 5=Safety';
COMMENT ON COLUMN dim_quality_defect.is_safety_critical IS 'TRUE if could trigger recall';


-- ================================================================
-- VERIFY DIMENSIONS CREATED
-- ================================================================

SELECT 
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_schema = 'warehouse' 
       AND information_schema.columns.table_name = tables.table_name) as column_count
FROM information_schema.tables
WHERE table_schema = 'warehouse'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- Expected output:
--      table_name          | column_count
-- -------------------------+--------------
--  dim_date                |     10
--  dim_equipment           |     11
--  dim_production_line     |      8
--  dim_quality_defect      |      9
--  dim_shift               |      7
--  dim_vehicle             |     11
