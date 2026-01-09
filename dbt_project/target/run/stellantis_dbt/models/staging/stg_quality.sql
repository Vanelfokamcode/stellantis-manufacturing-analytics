
  create view "stellantis_manufacturing"."dbt_dev_staging"."stg_quality__dbt_tmp"
    
    
  as (
    -- models/staging/stg_quality.sql
-- Quality sensor measurements

with source as (
    select * from "stellantis_manufacturing"."staging"."stg_quality"
),

renamed as (
    select
        -- identifiers
        row_id,
        
        -- quality result
        pass_fail,
        case 
            when pass_fail = 1 then 'PASS'
            when pass_fail = -1 then 'FAIL'
            else 'UNKNOWN'
        end as quality_status,
        
        -- sensor readings
        sensor_1,
        sensor_2,
        sensor_3,
        
        -- calculated
        (sensor_1 + sensor_2 + sensor_3) / 3.0 as avg_sensor_reading,
        
        -- metadata
        loaded_at as source_loaded_at,
        current_timestamp as dbt_loaded_at
    
    from source
)

select * from renamed
  );