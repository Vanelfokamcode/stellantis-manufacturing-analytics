
  create view "stellantis_manufacturing"."dbt_prod_staging"."stg_production__dbt_tmp"
    
    
  as (
    -- models/staging/stg_production.sql
-- Clean and standardize production data

with source as (
    select * from "stellantis_manufacturing"."staging"."stg_production_metrics"
),

renamed as (
    select
        -- identifiers
        line_name as production_line,
        production_date as date,
        shift_name as shift,
        vehicle_make,
        vehicle_model,
        vehicle_year,
        
        -- metrics
        units_produced,
        units_target,
        defects,
        downtime_minutes,
        cycle_time_seconds,
        oee_percent,
        
        -- calculated
        units_produced * 150 as estimated_revenue,
        downtime_minutes / 60.0 as downtime_hours,
        case 
            when units_produced > 0 
            then (defects::float / units_produced * 100)
            else 0 
        end as defect_rate_percent,
        
        -- metadata
        loaded_at as source_loaded_at,
        current_timestamp as dbt_loaded_at
    
    from source
)

select * from renamed
  );