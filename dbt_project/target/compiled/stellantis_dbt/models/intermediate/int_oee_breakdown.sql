-- models/intermediate/int_oee_breakdown.sql
-- Detailed OEE component analysis
-- OEE = Availability × Performance × Quality

with production as (
    select * from "stellantis_manufacturing"."dbt_prod_intermediate"."int_production_enriched"
),

oee_calc as (
    select
        production_line,
        date,
        shift,
        oee_percent,
        units_produced,
        units_target,
        defects,
        downtime_hours,
        cycle_time_seconds,
        estimated_revenue,
        
        -- Availability (uptime)
        ((480 - (downtime_hours * 60)) / 480.0 * 100) as availability_percent,
        
        -- Performance (speed)
        (units_produced::float / nullif(units_target, 0) * 100) as performance_percent,
        
        -- Quality (first pass yield)
        ((units_produced - defects)::float / nullif(units_produced, 0) * 100) as quality_percent,
        
        dbt_loaded_at
        
    from production
),

oee_components as (
    select
        *,
        
        -- Calculated OEE (for validation)
        (availability_percent / 100.0 * performance_percent / 100.0 * quality_percent / 100.0 * 100) as calculated_oee_percent,
        
        -- Loss categories
        downtime_hours * 60 as planned_downtime_min,
        
        case 
            when units_produced < units_target 
            then (units_target - units_produced) * cycle_time_seconds / 60.0
            else 0 
        end as speed_loss_min,
        
        defects * cycle_time_seconds / 60.0 as quality_loss_min,
        
        -- Bottleneck identification
        case 
            when availability_percent < 90 then 'AVAILABILITY'
            when performance_percent < 90 then 'PERFORMANCE'
            when quality_percent < 95 then 'QUALITY'
            else 'NONE'
        end as primary_bottleneck,
        
        -- Improvement potential
        (100 - oee_percent) * estimated_revenue / 100.0 as potential_revenue_gain
        
    from oee_calc
)

select * from oee_components