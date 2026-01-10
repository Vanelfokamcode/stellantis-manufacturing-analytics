
  
    

  create  table "stellantis_manufacturing"."dbt_prod_marts"."mart_maintenance_overview__dbt_tmp"
  
  
    as
  
  (
    -- models/marts/mart_maintenance_overview.sql
-- Machine health overview and failure prediction
-- Answers: "Which machines need attention? When will they fail?"

with maintenance_features as (
    select * from "stellantis_manufacturing"."dbt_prod_intermediate"."int_maintenance_features"
),

machine_aggregated as (
    select
        product_id,
        product_type,
        
        -- Aggregated metrics
        count(*) as total_observations,
        max(failure_risk_score) as max_risk_score,
        avg(failure_risk_score) as avg_risk_score,
        
        -- Latest readings (most recent)
        max(air_temperature_c) as latest_air_temp,
        max(process_temperature_c) as latest_process_temp,
        max(rotational_speed_rpm) as latest_speed,
        max(torque_nm) as latest_torque,
        max(tool_wear_min) as latest_tool_wear,
        max(power_kw) as latest_power,
        
        -- Average conditions
        avg(air_temperature_c) as avg_air_temp,
        avg(process_temperature_c) as avg_process_temp,
        avg(rotational_speed_rpm) as avg_speed,
        avg(torque_nm) as avg_torque,
        avg(tool_wear_min) as avg_tool_wear,
        
        -- Failure history
        sum(machine_failure) as total_failures,
        sum(tool_wear_failure) as tool_wear_failures,
        sum(heat_dissipation_failure) as heat_failures,
        sum(power_failure) as power_failures,
        sum(overstrain_failure) as overstrain_failures,
        sum(random_failure) as random_failures,
        
        -- Latest status (most recent record)
        mode() within group (order by wear_status) as current_wear_status,
        mode() within group (order by speed_category) as typical_speed_category,
        mode() within group (order by failure_type) as most_common_failure_type
        
    from maintenance_features
    group by product_id, product_type
),

with_risk_assessment as (
    select
        *,
        
        -- Failure rate
        (total_failures::float / nullif(total_observations, 0) * 100) as failure_rate_percent,
        
        -- Risk category based on multiple factors
        case 
            when max_risk_score >= 80 or latest_tool_wear >= 200 then 'CRITICAL'
            when max_risk_score >= 60 or latest_tool_wear >= 150 then 'HIGH'
            when max_risk_score >= 40 or latest_tool_wear >= 100 then 'MEDIUM'
            else 'LOW'
        end as risk_category,
        
        -- Maintenance urgency (days until recommended maintenance)
        case 
            when latest_tool_wear >= 200 then 0  -- Immediate
            when latest_tool_wear >= 180 then 7  -- Within a week
            when latest_tool_wear >= 150 then 14 -- Within 2 weeks
            when latest_tool_wear >= 120 then 30 -- Within a month
            else 60 -- Within 2 months
        end as days_until_maintenance,
        
        -- Dominant failure mode
        case 
            when tool_wear_failures > heat_failures 
                 and tool_wear_failures > power_failures 
                 and tool_wear_failures > overstrain_failures
                 then 'TOOL_WEAR'
            when heat_failures > power_failures 
                 and heat_failures > overstrain_failures
                 then 'HEAT_DISSIPATION'
            when power_failures > overstrain_failures
                 then 'POWER'
            when overstrain_failures > 0
                 then 'OVERSTRAIN'
            else 'RANDOM'
        end as dominant_failure_mode,
        
        -- Operating stress level
        case 
            when latest_power > 15 then 'HIGH_STRESS'
            when latest_power > 10 then 'MEDIUM_STRESS'
            else 'LOW_STRESS'
        end as operating_stress,
        
        -- Ranking by risk
        rank() over (order by max_risk_score desc) as risk_rank,
        rank() over (order by latest_tool_wear desc) as wear_rank,
        rank() over (order by total_failures desc) as failure_count_rank
        
    from machine_aggregated
),

with_recommendations as (
    select
        *,
        
        -- Specific action recommendations
        case 
            when risk_category = 'CRITICAL' then 'STOP PRODUCTION - Replace tool immediately'
            when risk_category = 'HIGH' and dominant_failure_mode = 'TOOL_WEAR' 
                 then 'Schedule tool replacement within 7 days'
            when risk_category = 'HIGH' and dominant_failure_mode = 'HEAT_DISSIPATION'
                 then 'Inspect cooling system within 7 days'
            when risk_category = 'HIGH' and dominant_failure_mode = 'POWER'
                 then 'Check electrical system within 7 days'
            when risk_category = 'MEDIUM' 
                 then 'Schedule preventive maintenance within 2 weeks'
            else 'Continue monitoring - routine maintenance'
        end as action_recommendation,
        
        -- Estimated cost if failure occurs (vs preventive maintenance)
        case 
            when risk_category = 'CRITICAL' then 50000  -- Emergency repair
            when risk_category = 'HIGH' then 25000
            when risk_category = 'MEDIUM' then 10000
            else 5000
        end as estimated_failure_cost,
        
        -- Preventive maintenance cost
        case 
            when dominant_failure_mode = 'TOOL_WEAR' then 2000
            when dominant_failure_mode = 'HEAT_DISSIPATION' then 3000
            when dominant_failure_mode = 'POWER' then 5000
            else 1500
        end as preventive_maintenance_cost,
        
        -- Cost savings (failure cost - preventive cost)
        case 
            when risk_category = 'CRITICAL' then 50000 - 2000
            when risk_category = 'HIGH' then 25000 - 2000
            when risk_category = 'MEDIUM' then 10000 - 1500
            else 0
        end as potential_cost_savings
        
    from with_risk_assessment
)

select
    -- Machine identification
    product_id,
    product_type,
    
    -- Observation count
    total_observations,
    
    -- Current status (latest readings)
    latest_air_temp,
    latest_process_temp,
    latest_speed,
    latest_torque,
    latest_tool_wear,
    latest_power,
    current_wear_status,
    operating_stress,
    
    -- Average conditions
    avg_air_temp,
    avg_process_temp,
    avg_speed,
    avg_torque,
    avg_tool_wear,
    
    -- Risk assessment
    max_risk_score,
    avg_risk_score,
    risk_category,
    days_until_maintenance,
    
    -- Failure history
    total_failures,
    failure_rate_percent,
    tool_wear_failures,
    heat_failures,
    power_failures,
    overstrain_failures,
    random_failures,
    dominant_failure_mode,
    most_common_failure_type,
    
    -- Rankings
    risk_rank,
    wear_rank,
    failure_count_rank,
    
    -- Financial impact
    estimated_failure_cost,
    preventive_maintenance_cost,
    potential_cost_savings,
    
    -- Recommendations
    action_recommendation,
    
    -- Priority flag for dashboard
    case 
        when risk_category = 'CRITICAL' then 1
        when risk_category = 'HIGH' then 2
        when risk_category = 'MEDIUM' then 3
        else 4
    end as priority_order,
    
    -- Metadata
    current_timestamp as mart_refreshed_at

from with_recommendations
order by priority_order, max_risk_score desc
  );
  