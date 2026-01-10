
  
    

  create  table "stellantis_manufacturing"."dbt_prod_marts"."mart_executive_kpis__dbt_tmp"
  
  
    as
  
  (
    -- models/marts/mart_executive_kpis.sql
-- Daily executive dashboard - ONE row per day
-- Answers: "How did the factory perform TODAY?"

with daily_production as (
    -- Aggregate ALL production lines for each day
    select
        date,
        
        -- Volume metrics
        count(distinct production_line) as active_lines,
        count(*) as total_shifts_run,  -- Total shift records
        sum(units_produced) as total_units_produced,
        sum(units_target) as total_units_target,
        
        -- Performance metrics
        avg(oee_percent) as avg_oee,
        min(oee_percent) as worst_oee,
        max(oee_percent) as best_oee,
        
        -- Quality metrics
        sum(defects) as total_defects,
        avg(defect_rate_percent) as avg_defect_rate,
        
        -- Time metrics
        sum(downtime_hours) as total_downtime_hours,
        avg(cycle_time_seconds) as avg_cycle_time,
        
        -- Cost metrics (from int_production_enriched)
        sum(defect_cost) as total_defect_cost,
        sum(downtime_cost) as total_downtime_cost,
        sum(total_loss) as total_loss,
        sum(estimated_revenue) as total_revenue
        
    from "stellantis_manufacturing"."dbt_prod_intermediate"."int_production_enriched"
    group by date
),

daily_with_calcs as (
    select
        date,
        active_lines,
        total_shifts_run,
        total_units_produced,
        total_units_target,
        
        -- Performance
        avg_oee,
        worst_oee,
        best_oee,
        
        -- Target achievement
        (total_units_produced::float / nullif(total_units_target, 0) * 100) as target_achievement_percent,
        (total_units_target - total_units_produced) as units_below_target,
        
        -- Quality
        total_defects,
        avg_defect_rate,
        (total_defects::float / nullif(total_units_produced, 0) * 100) as overall_defect_rate,
        
        -- Efficiency
        total_downtime_hours,
        total_downtime_hours / nullif(active_lines, 0) as avg_downtime_per_line,
        avg_cycle_time,
        
        -- Costs & Revenue
        total_defect_cost,
        total_downtime_cost,
        total_loss,
        total_revenue,
        total_revenue - total_loss as net_revenue,
        (total_loss / nullif(total_revenue, 0) * 100) as loss_percentage,
        
        -- Performance classification
        case 
            when avg_oee >= 85 then 'EXCELLENT'
            when avg_oee >= 75 then 'GOOD'
            when avg_oee >= 65 then 'ACCEPTABLE'
            else 'CRITICAL'
        end as factory_health_status
        
    from daily_production
),

with_trends as (
    select
        *,
        
        -- Trend vs previous day (using window functions)
        lag(total_units_produced) over (order by date) as prev_day_units,
        lag(avg_oee) over (order by date) as prev_day_oee,
        
        -- Calculate change
        total_units_produced - lag(total_units_produced) over (order by date) as units_change_vs_yesterday,
        avg_oee - lag(avg_oee) over (order by date) as oee_change_vs_yesterday,
        
        -- 7-day rolling averages (for trend smoothing)
        avg(avg_oee) over (
            order by date 
            rows between 6 preceding and current row
        ) as rolling_7day_avg_oee,
        
        avg(total_units_produced) over (
            order by date 
            rows between 6 preceding and current row
        ) as rolling_7day_avg_production
        
    from daily_with_calcs
)

select
    -- Date
    date,
    
    -- Volume
    active_lines,
    total_shifts_run,
    total_units_produced,
    total_units_target,
    target_achievement_percent,
    units_below_target,
    
    -- Performance
    avg_oee,
    worst_oee,
    best_oee,
    factory_health_status,
    
    -- Quality
    total_defects,
    overall_defect_rate,
    avg_defect_rate,
    
    -- Efficiency
    total_downtime_hours,
    avg_downtime_per_line,
    avg_cycle_time,
    
    -- Financial
    total_revenue,
    total_defect_cost,
    total_downtime_cost,
    total_loss,
    net_revenue,
    loss_percentage,
    
    -- Trends
    units_change_vs_yesterday,
    oee_change_vs_yesterday,
    rolling_7day_avg_oee,
    rolling_7day_avg_production,
    
    -- Metadata
    current_timestamp as mart_refreshed_at

from with_trends
order by date desc
  );
  