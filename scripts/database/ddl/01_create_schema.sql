-- ================================================================
-- STELLANTIS MANUFACTURING ANALYTICS
-- Script: Create Database Schemas
-- Author: Vanel
-- Date: January 2025
-- Description: Creates logical schemas for data organization
-- ================================================================

-- Drop schemas if exist (for clean re-runs)
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS warehouse CASCADE;

-- Create schemas
CREATE SCHEMA staging;
CREATE SCHEMA warehouse;

-- Add comments (documentation in DB)
COMMENT ON SCHEMA staging IS 'Raw data landing zone from CSV files';
COMMENT ON SCHEMA warehouse IS 'Star schema for analytics (fact + dimensions)';

-- Grant permissions (if using roles)
-- GRANT USAGE ON SCHEMA staging TO data_engineer;
-- GRANT USAGE ON SCHEMA warehouse TO analyst;

-- Verify
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name IN ('staging', 'warehouse');

-- Expected output:
--  schema_name
-- -------------
--  staging
--  warehouse
