-- models/staging/stg_maintenance.sql
-- Predictive maintenance sensor data

with source as (
    select * from "stellantis_manufacturing"."staging"."stg_maintenance"
),

renamed as (
    select
        -- identifiers
        udi as unit_device_id,
        product_id,
        type as product_type,
        
        -- sensor readings
        air_temperature_k,
        process_temperature_k,
        rotational_speed_rpm,
        torque_nm,
        tool_wear_min,
        
        -- failure indicators
        machine_failure,
        twf as tool_wear_failure,
        hdf as heat_dissipation_failure,
        pwf as power_failure,
        osf as overstrain_failure,
        rnf as random_failure,
        
        -- calculated
        air_temperature_k - 273.15 as air_temperature_c,
        process_temperature_k - 273.15 as process_temperature_c,
        
        -- metadata
        loaded_at as source_loaded_at,
        current_timestamp as dbt_loaded_at
    
    from source
)

select * from renamed