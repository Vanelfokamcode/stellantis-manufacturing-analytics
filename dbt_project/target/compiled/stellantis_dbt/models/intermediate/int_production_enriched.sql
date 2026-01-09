-- models/intermediate/int_production_enriched.sql
-- Production data enriched with business logic and classifications

with production as (
    select * from "stellantis_manufacturing"."dbt_dev_staging"."stg_production"
),

enriched as (
    select
        -- original fields
        production_line,
        date,
        shift,
        vehicle_make,
        vehicle_model,
        vehicle_year,
        units_produced,
        units_target,
        defects,
        downtime_hours,
        cycle_time_seconds,
        oee_percent,
        defect_rate_percent,
        estimated_revenue,
        
        -- performance classification
        case 
            when oee_percent >= 85 then 'EXCELLENT'
            when oee_percent >= 70 then 'GOOD'
            when oee_percent >= 60 then 'ACCEPTABLE'
            else 'NEEDS_ATTENTION'
        end as oee_category,
        
        -- production efficiency
        case 
            when units_produced >= units_target then 'MET_TARGET'
            when units_produced >= units_target * 0.9 then 'NEAR_TARGET'
            else 'BELOW_TARGET'
        end as target_achievement,
        
        -- quality classification
        case 
            when defect_rate_percent <= 1.0 then 'EXCELLENT'
            when defect_rate_percent <= 3.0 then 'GOOD'
            when defect_rate_percent <= 5.0 then 'ACCEPTABLE'
            else 'POOR'
        end as quality_category,
        
        -- shift productivity score (weighted)
        (oee_percent * 0.5) + 
        ((units_produced::float / units_target * 100) * 0.3) +
        ((100 - defect_rate_percent) * 0.2) as productivity_score,
        
        -- downtime impact
        case 
            when downtime_hours <= 0.5 then 'LOW'
            when downtime_hours <= 1.0 then 'MEDIUM'
            else 'HIGH'
        end as downtime_impact,
        
        -- cost calculations
        defects * 50 as defect_cost,
        downtime_hours * 5000 as downtime_cost,
        (defects * 50) + (downtime_hours * 5000) as total_loss,
        
        -- metadata
        dbt_loaded_at
        
    from production
)

select * from enriched