-- models/marts/mart_line_performance.sql
-- Production line rankings and performance comparison
-- Answers: "Which lines perform best/worst?"

with line_aggregated as (
    select
        production_line,
        
        -- Time period
        min(date) as first_production_date,
        max(date) as last_production_date,
        count(distinct date) as days_active,
        count(*) as total_shifts,
        
        -- Volume metrics
        sum(units_produced) as total_units_produced,
        sum(units_target) as total_units_target,
        avg(units_produced) as avg_units_per_shift,
        
        -- Performance metrics
        avg(oee_percent) as avg_oee,
        min(oee_percent) as worst_oee,
        max(oee_percent) as best_oee,
        stddev(oee_percent) as oee_std_dev,  -- Consistency metric
        
        -- Quality metrics
        sum(defects) as total_defects,
        avg(defect_rate_percent) as avg_defect_rate,
        
        -- Efficiency
        sum(downtime_hours) as total_downtime_hours,
        avg(cycle_time_seconds) as avg_cycle_time,
        
        -- Financial
        sum(estimated_revenue) as total_revenue,
        sum(total_loss) as total_loss,
        
        -- Categories (most frequent)
        mode() within group (order by oee_category) as typical_oee_category,
        mode() within group (order by quality_category) as typical_quality_category
        
    from {{ ref('int_production_enriched') }}
    group by production_line
),

with_rankings as (
    select
        *,
        
        -- Target achievement
        (total_units_produced::float / nullif(total_units_target, 0) * 100) as target_achievement_percent,
        
        -- Downtime per day
        total_downtime_hours / nullif(days_active, 0) as avg_downtime_hours_per_day,
        
        -- Rankings (1 = best)
        rank() over (order by avg_oee desc) as oee_rank,
        rank() over (order by total_units_produced desc) as production_volume_rank,
        rank() over (order by avg_defect_rate asc) as quality_rank,
        rank() over (order by total_downtime_hours asc) as uptime_rank,
        
        -- Percentile scores (0-100, higher = better)
        percent_rank() over (order by avg_oee) * 100 as oee_percentile,
        percent_rank() over (order by total_units_produced) * 100 as volume_percentile,
        
        -- Consistency score (lower std dev = more consistent)
        case 
            when oee_std_dev = 0 then 100
            else 100 - (oee_std_dev / (select max(oee_std_dev) from line_aggregated) * 100)
        end as consistency_score
        
    from line_aggregated
),

with_improvement as (
    select
        *,
        
        -- Improvement potential (vs best line)
        max(avg_oee) over () - avg_oee as oee_gap_to_best,
        
        -- Revenue opportunity if this line matched best OEE
        ((max(avg_oee) over () - avg_oee) / 100.0 * total_revenue) as potential_revenue_gain,
        
        -- Overall score (weighted: OEE 50%, Quality 30%, Volume 20%)
        (oee_percentile * 0.5 + 
         (100 - percent_rank() over (order by avg_defect_rate) * 100) * 0.3 +
         volume_percentile * 0.2) as overall_performance_score
        
    from with_rankings
)

select
    -- Line identification
    production_line,
    
    -- Time metrics
    first_production_date,
    last_production_date,
    days_active,
    total_shifts,
    
    -- Volume
    total_units_produced,
    total_units_target,
    target_achievement_percent,
    avg_units_per_shift,
    
    -- Performance
    avg_oee,
    worst_oee,
    best_oee,
    oee_std_dev,
    typical_oee_category,
    
    -- Quality
    total_defects,
    avg_defect_rate,
    typical_quality_category,
    
    -- Efficiency
    total_downtime_hours,
    avg_downtime_hours_per_day,
    avg_cycle_time,
    
    -- Financial
    total_revenue,
    total_loss,
    
    -- Rankings & Scores
    oee_rank,
    production_volume_rank,
    quality_rank,
    uptime_rank,
    consistency_score,
    overall_performance_score,
    
    -- Improvement potential
    oee_gap_to_best,
    potential_revenue_gain,
    
    -- Action priority
    case 
        when overall_performance_score < 30 then 'CRITICAL - Immediate Action'
        when overall_performance_score < 50 then 'HIGH - Schedule Review'
        when overall_performance_score < 70 then 'MEDIUM - Monitor Closely'
        else 'LOW - Maintain Performance'
    end as priority_level,
    
    -- Metadata
    current_timestamp as mart_refreshed_at

from with_improvement
order by overall_performance_score desc
