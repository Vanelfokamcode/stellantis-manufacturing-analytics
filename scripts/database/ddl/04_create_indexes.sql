-- ================================================================
-- STELLANTIS MANUFACTURING ANALYTICS
-- Script: Create Indexes for Performance
-- Author: Vanel
-- Date: January 2025
-- Description: Speed up common query patterns
-- Business: Sub-second dashboard response times
-- ================================================================

SET search_path TO warehouse;

-- ================================================================
-- FACT TABLE INDEXES
-- ================================================================

-- Most common filter: date range queries
CREATE INDEX idx_fact_date 
    ON fact_production_metrics(date_key);

-- Common grouping: by production line
CREATE INDEX idx_fact_line 
    ON fact_production_metrics(line_key);

-- Common grouping: by vehicle
CREATE INDEX idx_fact_vehicle 
    ON fact_production_metrics(vehicle_key);

-- Common grouping: by shift
CREATE INDEX idx_fact_shift 
    ON fact_production_metrics(shift_key);

-- Composite index for common query pattern:
-- "Production by line and date"
CREATE INDEX idx_fact_line_date 
    ON fact_production_metrics(line_key, date_key);

-- Audit queries: find recent inserts
CREATE INDEX idx_fact_created 
    ON fact_production_metrics(created_at);


-- ================================================================
-- DIMENSION INDEXES
-- ================================================================

-- dim_date: often filtered by year/quarter
CREATE INDEX idx_date_year 
    ON dim_date(year);
    
CREATE INDEX idx_date_quarter 
    ON dim_date(quarter, year);

-- dim_vehicle: often filtered by make
CREATE INDEX idx_vehicle_make 
    ON dim_vehicle(make);

-- dim_equipment: often filtered by type
CREATE INDEX idx_equipment_type 
    ON dim_equipment(equipment_type);


-- ================================================================
-- VERIFY INDEXES
-- ================================================================

SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'warehouse'
ORDER BY tablename, indexname;

-- Expected: ~12 indexes total
