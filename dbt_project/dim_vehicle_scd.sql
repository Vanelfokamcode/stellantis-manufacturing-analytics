-- models/dimensions/dim_vehicle_scd.sql
-- Vehicle dimension with SCD Type 2 concept
-- Simplified version for Day 25 demo

{{
    config(
        materialized='table'
    )
}}

with source_data as (
    select distinct
        vehicle_make as make,
        vehicle_model as model,
        vehicle_year as year,
        min(production_date) as first_seen_date,
        max(production_date) as last_seen_date,
        -- Estimated MSRP (placeholder - in reality from pricing table)
        case 
            when vehicle_make like '%Peugeot%' then 28000
            when vehicle_make like '%Jeep%' then 35000
            when vehicle_make like '%Fiat%' then 22000
            else 30000
        end as current_msrp
    from {{ ref('stg_production') }}
    group by vehicle_make, vehicle_model, vehicle_year
),

with_scd_structure as (
    select
        {{ dbt_utils.generate_surrogate_key(['make', 'model', 'year']) }} as vehicle_key,
        make,
        model,
        year,
        current_msrp as msrp,
        
        -- SCD Type 2 columns
        first_seen_date as valid_from,
        null::date as valid_to,  -- NULL means currently valid
        true as is_current,
        
        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
    from source_data
)

select
    vehicle_key,
    make,
    model,
    year,
    msrp,
    valid_from,
    valid_to,
    is_current,
    created_at,
    updated_at,
    
    -- Business classification
    case 
        when make like '%Peugeot%' then 'ECONOMY'
        when make like '%Jeep%' then 'SUV'
        when make like '%Fiat%' then 'COMPACT'
        else 'OTHER'
    end as vehicle_category,
    
    -- Example of how future updates would work:
    -- When MSRP changes from 35000 to 37000:
    -- 1. Set current record: valid_to = '2024-06-30', is_current = false
    -- 2. Insert new record: msrp = 37000, valid_from = '2024-07-01', is_current = true
    
    'Initial load - Future updates would create new versions' as scd_note

from with_scd_structure
order by make, model, year
