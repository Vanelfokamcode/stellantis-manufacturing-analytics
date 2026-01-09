-- models/intermediate/int_quality_aggregated.sql
-- Quality metrics aggregated for analysis

with quality as (
    select * from {{ ref('stg_quality') }}
),

aggregated as (
    select
        row_id,
        pass_fail,
        quality_status,
        sensor_1,
        sensor_2,
        sensor_3,
        avg_sensor_reading,
        
        -- sensor variance analysis
        greatest(sensor_1, sensor_2, sensor_3) - 
        least(sensor_1, sensor_2, sensor_3) as sensor_range,
        
        -- individual sensor deviation from average
        abs(sensor_1 - avg_sensor_reading) as sensor_1_deviation,
        abs(sensor_2 - avg_sensor_reading) as sensor_2_deviation,
        abs(sensor_3 - avg_sensor_reading) as sensor_3_deviation,
        
        -- quality confidence
        case 
            when (greatest(sensor_1, sensor_2, sensor_3) - 
                  least(sensor_1, sensor_2, sensor_3)) < 100 
            then 'HIGH_CONFIDENCE'
            when (greatest(sensor_1, sensor_2, sensor_3) - 
                  least(sensor_1, sensor_2, sensor_3)) < 200 
            then 'MEDIUM_CONFIDENCE'
            else 'LOW_CONFIDENCE'
        end as measurement_confidence,
        
        -- anomaly detection (simple threshold-based)
        case 
            when avg_sensor_reading > 3500 or avg_sensor_reading < 2000 
            then true 
            else false 
        end as is_anomaly,
        
        dbt_loaded_at
        
    from quality
)

select * from aggregated
