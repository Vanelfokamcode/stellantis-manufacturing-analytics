
  
    

  create  table "stellantis_manufacturing"."dbt_dev_marts"."mart_cost_analysis__dbt_tmp"
  
  
    as
  
  (
    -- models/marts/mart_cost_analysis.sql
-- Cost analysis and financial impact breakdown
-- Answers: "Where are we losing money? What's the ROI of improvements?"

with production_costs as (
    select
        date,
        production_line,
        shift,
        
        -- Revenue
        estimated_revenue,
        
        -- Cost components
        defect_cost,
        downtime_cost,
        total_loss,
        
        -- Volume metrics for context
        units_produced,
        units_target,
        defects,
        downtime_hours,
        
        -- Performance context
        oee_percent,
        oee_category
        
    from "stellantis_manufacturing"."dbt_dev_intermediate"."int_production_enriched"
),

aggregated_costs as (
    select
        date,
        
        -- Total costs by category
        sum(defect_cost) as total_defect_cost,
        sum(downtime_cost) as total_downtime_cost,
        sum(total_loss) as total_loss,
        sum(estimated_revenue) as total_revenue,
        
        -- Volume context
        sum(units_produced) as total_units,
        sum(defects) as total_defects,
        sum(downtime_hours) as total_downtime_hours,
        
        -- Count by category
        count(*) filter (where oee_category = 'NEEDS_ATTENTION') as poor_performance_shifts,
        count(*) as total_shifts,
        
        -- By line breakdown
        count(distinct production_line) as active_lines
        
    from production_costs
    group by date
),

with_calculations as (
    select
        date,
        
        -- Revenue & costs
        total_revenue,
        total_defect_cost,
        total_downtime_cost,
        total_loss,
        total_revenue - total_loss as net_revenue,
        
        -- Loss breakdown percentages
        (total_defect_cost::float / nullif(total_loss, 0) * 100) as defect_cost_percentage,
        (total_downtime_cost::float / nullif(total_loss, 0) * 100) as downtime_cost_percentage,
        
        -- Loss as % of revenue
        (total_loss::float / nullif(total_revenue, 0) * 100) as loss_percentage_of_revenue,
        
        -- Per unit costs
        (total_defect_cost::float / nullif(total_units, 0)) as defect_cost_per_unit,
        (total_downtime_cost::float / nullif(total_units, 0)) as downtime_cost_per_unit,
        (total_loss::float / nullif(total_units, 0)) as total_loss_per_unit,
        
        -- Volume metrics
        total_units,
        total_defects,
        total_downtime_hours,
        active_lines,
        total_shifts,
        poor_performance_shifts,
        
        -- Performance context
        (poor_performance_shifts::float / nullif(total_shifts, 0) * 100) as poor_performance_percentage,
        
        -- Potential savings (if all shifts were GOOD or better)
        total_loss * (poor_performance_shifts::float / nullif(total_shifts, 0)) as potential_savings_from_improvement
        
    from aggregated_costs
),

with_trends as (
    select
        *,
        
        -- Cost trends
        lag(total_loss) over (order by date) as prev_day_loss,
        total_loss - lag(total_loss) over (order by date) as loss_change_vs_yesterday,
        
        -- Rolling averages (7-day)
        avg(total_loss) over (
            order by date
            rows between 6 preceding and current row
        ) as rolling_7day_avg_loss,
        
        avg(total_defect_cost) over (
            order by date
            rows between 6 preceding and current row
        ) as rolling_7day_avg_defect_cost,
        
        avg(total_downtime_cost) over (
            order by date
            rows between 6 preceding and current row
        ) as rolling_7day_avg_downtime_cost,
        
        -- Month-to-date cumulative
        sum(total_loss) over (
            partition by date_trunc('month', date)
            order by date
        ) as mtd_cumulative_loss,
        
        sum(total_revenue) over (
            partition by date_trunc('month', date)
            order by date
        ) as mtd_cumulative_revenue,
        
        -- Year-to-date cumulative
        sum(total_loss) over (
            partition by date_trunc('year', date)
            order by date
        ) as ytd_cumulative_loss
        
    from with_calculations
),

with_priorities as (
    select
        *,
        
        -- MTD metrics
        (mtd_cumulative_loss::float / nullif(mtd_cumulative_revenue, 0) * 100) as mtd_loss_percentage,
        
        -- Primary cost driver
        case 
            when total_defect_cost > total_downtime_cost then 'DEFECTS'
            when total_downtime_cost > total_defect_cost then 'DOWNTIME'
            else 'BALANCED'
        end as primary_cost_driver,
        
        -- Cost level severity
        case 
            when loss_percentage_of_revenue > 10 then 'CRITICAL'
            when loss_percentage_of_revenue > 5 then 'HIGH'
            when loss_percentage_of_revenue > 3 then 'MEDIUM'
            else 'LOW'
        end as cost_severity,
        
        -- ROI opportunity (estimated annual if we reduce loss by 50%)
        (total_loss * 0.5 * 365) as annual_roi_opportunity_50pct_reduction
        
    from with_trends
)

select
    -- Date
    date,
    
    -- Revenue & Net
    total_revenue,
    total_loss,
    net_revenue,
    loss_percentage_of_revenue,
    
    -- Cost breakdown
    total_defect_cost,
    total_downtime_cost,
    defect_cost_percentage,
    downtime_cost_percentage,
    primary_cost_driver,
    
    -- Per unit economics
    defect_cost_per_unit,
    downtime_cost_per_unit,
    total_loss_per_unit,
    
    -- Volume context
    total_units,
    total_defects,
    total_downtime_hours,
    active_lines,
    total_shifts,
    poor_performance_shifts,
    poor_performance_percentage,
    
    -- Improvement potential
    potential_savings_from_improvement,
    annual_roi_opportunity_50pct_reduction,
    
    -- Trends
    loss_change_vs_yesterday,
    rolling_7day_avg_loss,
    rolling_7day_avg_defect_cost,
    rolling_7day_avg_downtime_cost,
    
    -- Cumulative
    mtd_cumulative_loss,
    mtd_cumulative_revenue,
    mtd_loss_percentage,
    ytd_cumulative_loss,
    
    -- Severity & Priority
    cost_severity,
    
    -- Action recommendation
    case 
        when cost_severity = 'CRITICAL' and primary_cost_driver = 'DOWNTIME'
            then 'URGENT: Focus on reducing downtime - maintenance review needed'
        when cost_severity = 'CRITICAL' and primary_cost_driver = 'DEFECTS'
            then 'URGENT: Quality improvement program required'
        when cost_severity = 'HIGH' and primary_cost_driver = 'DOWNTIME'
            then 'HIGH: Schedule maintenance optimization workshop'
        when cost_severity = 'HIGH' and primary_cost_driver = 'DEFECTS'
            then 'HIGH: Implement quality control improvements'
        when poor_performance_percentage > 30
            then 'MEDIUM: Training program for underperforming shifts'
        else 'LOW: Continue monitoring and preventive measures'
    end as recommended_action,
    
    -- Metadata
    current_timestamp as mart_refreshed_at

from with_priorities
order by date desc
  );
  