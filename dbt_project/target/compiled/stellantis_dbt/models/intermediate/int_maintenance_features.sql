-- models/intermediate/int_maintenance_features.sql
-- Machine health indicators and failure risk assessment

with maintenance as (
    select * from "stellantis_manufacturing"."dbt_dev_staging"."stg_maintenance"
),

features as (
    select
        unit_device_id,
        product_id,
        product_type,
        
        -- sensor readings
        air_temperature_c,
        process_temperature_c,
        rotational_speed_rpm,
        torque_nm,
        tool_wear_min,
        
        -- failure flags
        machine_failure,
        tool_wear_failure,
        heat_dissipation_failure,
        power_failure,
        overstrain_failure,
        random_failure,
        
        -- calculated features
        process_temperature_c - air_temperature_c as temp_differential,
        
        -- operating conditions
        case 
            when air_temperature_c > 30 then 'HIGH_TEMP'
            when air_temperature_c < 15 then 'LOW_TEMP'
            else 'NORMAL_TEMP'
        end as ambient_condition,
        
        -- speed classification
        case 
            when rotational_speed_rpm > 2000 then 'HIGH_SPEED'
            when rotational_speed_rpm > 1500 then 'MEDIUM_SPEED'
            else 'LOW_SPEED'
        end as speed_category,
        
        -- wear assessment
        case 
            when tool_wear_min > 200 then 'CRITICAL'
            when tool_wear_min > 150 then 'HIGH'
            when tool_wear_min > 100 then 'MODERATE'
            else 'LOW'
        end as wear_status,
        
        -- power metric (torque × speed)
        torque_nm * rotational_speed_rpm / 9550.0 as power_kw,
        
        -- risk score (weighted sum of conditions)
        (
            case when tool_wear_min > 200 then 40 else tool_wear_min / 5.0 end +
            case when process_temperature_c > 40 then 30 else 0 end +
            case when rotational_speed_rpm > 2000 then 20 else 0 end +
            case when torque_nm > 60 then 10 else 0 end
        ) as failure_risk_score,
        
        -- failure type indicator
        case 
            when tool_wear_failure = 1 then 'TOOL_WEAR'
            when heat_dissipation_failure = 1 then 'HEAT'
            when power_failure = 1 then 'POWER'
            when overstrain_failure = 1 then 'OVERSTRAIN'
            when random_failure = 1 then 'RANDOM'
            when machine_failure = 1 then 'GENERAL'
            else 'NO_FAILURE'
        end as failure_type,
        
        dbt_loaded_at
        
    from maintenance
)

select * from features