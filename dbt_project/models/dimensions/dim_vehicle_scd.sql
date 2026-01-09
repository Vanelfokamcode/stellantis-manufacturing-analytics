-- models/dimensions/dim_vehicle_scd.sql
-- Vehicle dimension with SCD Type 2 structure

{{ config(materialized='table') }}

with source_data as (
    select distinct
        vehicle_make as make,
        vehicle_model as model,
        vehicle_year as year,
        min(date) as first_seen_date,
        max(date) as last_seen_date,
        case 
            when vehicle_make like '%Peugeot%' then 28000
            when vehicle_make like '%Jeep%' then 35000
            else 30000
        end as msrp
    from {{ ref('stg_production') }}
    group by vehicle_make, vehicle_model, vehicle_year
)

select
    {{ dbt_utils.generate_surrogate_key(['make', 'model', 'year']) }} as vehicle_key,
    make,
    model,
    year,
    msrp,
    first_seen_date as valid_from,
    null::date as valid_to,
    true as is_current,
    current_timestamp as created_at
from source_data
