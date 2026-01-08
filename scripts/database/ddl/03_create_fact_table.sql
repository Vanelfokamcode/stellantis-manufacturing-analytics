-- ================================================================
-- STELLANTIS MANUFACTURING ANALYTICS
-- Script: Create Fact Table
-- Author: Vanel
-- Date: January 2025
-- Description: Central fact table for production metrics
-- Business: Store measurable production performance data
-- ================================================================

SET search_path TO warehouse;

-- ================================================================
-- FACT TABLE: PRODUCTION METRICS
-- Business: Daily production performance by line/shift/vehicle
-- Grain: One row per production line, per shift, per day
-- Rows: ~5,475/year (5 lines × 3 shifts × 365 days)
-- ================================================================

CREATE TABLE fact_production_metrics (
    -- Primary Key
    metric_id SERIAL PRIMARY KEY,
    
    -- Foreign Keys (context dimensions)
    date_key DATE NOT NULL,
    line_key INTEGER NOT NULL,
    vehicle_key INTEGER NOT NULL,
    shift_key INTEGER NOT NULL,
    equipment_key INTEGER,  -- Nullable (some production = multiple equipment)
    
    -- Production metrics
    units_produced INTEGER NOT NULL CHECK (units_produced >= 0),
    units_target INTEGER NOT NULL CHECK (units_target >= 0),
    
    -- Quality metrics
    defects INTEGER NOT NULL CHECK (defects >= 0),
    
    -- Availability metrics
    downtime_minutes DECIMAL(10,2) NOT NULL CHECK (downtime_minutes >= 0),
    
    -- Performance metrics
    cycle_time_seconds DECIMAL(10,2) CHECK (cycle_time_seconds > 0),
    
    -- Calculated KPIs
    oee_percent DECIMAL(5,2) NOT NULL CHECK (oee_percent BETWEEN 0 AND 100),
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- Foreign Key Constraints
    CONSTRAINT fk_date 
        FOREIGN KEY (date_key) 
        REFERENCES dim_date(date)
        ON DELETE RESTRICT,
        
    CONSTRAINT fk_line 
        FOREIGN KEY (line_key) 
        REFERENCES dim_production_line(line_key)
        ON DELETE RESTRICT,
        
    CONSTRAINT fk_vehicle 
        FOREIGN KEY (vehicle_key) 
        REFERENCES dim_vehicle(vehicle_key)
        ON DELETE RESTRICT,
        
    CONSTRAINT fk_shift 
        FOREIGN KEY (shift_key) 
        REFERENCES dim_shift(shift_key)
        ON DELETE RESTRICT,
        
    CONSTRAINT fk_equipment 
        FOREIGN KEY (equipment_key) 
        REFERENCES dim_equipment(equipment_key)
        ON DELETE SET NULL,
        
    -- Business rule: can't have more defects than units produced
    CONSTRAINT chk_defects_vs_production 
        CHECK (defects <= units_produced),
        
    -- Unique constraint: one row per line/shift/day
    UNIQUE(date_key, line_key, shift_key)
);

-- Add table comment
COMMENT ON TABLE fact_production_metrics IS 'Production performance metrics (fact table)';

-- Add column comments
COMMENT ON COLUMN fact_production_metrics.oee_percent IS 'Overall Equipment Effectiveness = Availability × Performance × Quality';
COMMENT ON COLUMN fact_production_metrics.units_target IS 'Planned production (for variance analysis)';
COMMENT ON COLUMN fact_production_metrics.equipment_key IS 'Nullable if production uses multiple equipment';


-- ================================================================
-- VERIFY FACT TABLE CREATED
-- ================================================================

SELECT 
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_schema = 'warehouse' 
       AND information_schema.columns.table_name = tables.table_name) as column_count,
    (SELECT COUNT(*) FROM information_schema.table_constraints
     WHERE table_schema = 'warehouse'
       AND table_name = tables.table_name
       AND constraint_type = 'FOREIGN KEY') as fk_count
FROM information_schema.tables
WHERE table_schema = 'warehouse'
  AND table_name = 'fact_production_metrics';

