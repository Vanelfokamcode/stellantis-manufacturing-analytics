
  
    

  create  table "stellantis_manufacturing"."dbt_prod_marts"."mart_shift_analysis__dbt_tmp"
  
  
    as
  
  (
    -- models/marts/mart_shift_analysis.sql
-- Shift performance comparison and optimization
-- Answers: "Which shifts perform best? How can we optimize staffing?"

with shift_aggregated as (
    select
        shift,
        
        -- Volume
        count(*) as total_records,
        count(distinct production_line) as lines_operated,
        count(distinct date) as days_active,
        sum(units_produced) as total_units_produced,
        sum(units_target) as total_units_target,
        avg(units_produced) as avg_units_per_shift,
        
        -- Performance
        avg(oee_percent) as avg_oee,
        min(oee_percent) as worst_oee,
        max(oee_percent) as best_oee,
        stddev(oee_percent) as oee_variability,
        
        -- Quality
        sum(defects) as total_defects,
        avg(defect_rate_percent) as avg_defect_rate,
        
        -- Efficiency
        sum(downtime_hours) as total_downtime_hours,
        avg(downtime_hours) as avg_downtime_per_record,
        avg(cycle_time_seconds) as avg_cycle_time,
        
        -- Financial
        sum(estimated_revenue) as total_revenue,
        sum(total_loss) as total_loss,
        sum(defect_cost) as total_defect_cost,
        sum(downtime_cost) as total_downtime_cost,
        
        -- Categories
        count(*) filter (where oee_category = 'EXCELLENT') as excellent_shifts,
        count(*) filter (where oee_category = 'GOOD') as good_shifts,
        count(*) filter (where oee_category = 'NEEDS_ATTENTION') as poor_shifts
        
    from "stellantis_manufacturing"."dbt_prod_intermediate"."int_production_enriched"
    group by shift
),

with_calcs as (
    select
        *,
        
        -- Achievement rates
        (total_units_produced::float / nullif(total_units_target, 0) * 100) as target_achievement_percent,
        (total_defects::float / nullif(total_units_produced, 0) * 100) as overall_defect_rate,
        
        -- Per-day metrics
        total_units_produced / nullif(days_active, 0) as avg_units_per_day,
        total_downtime_hours / nullif(days_active, 0) as avg_downtime_per_day,
        
        -- Revenue metrics
        total_revenue - total_loss as net_revenue,
        (total_loss / nullif(total_revenue, 0) * 100) as loss_percentage,
        
        -- Productivity per hour (assuming 8-hour shifts)
        total_units_produced / nullif(total_records * 8.0, 0) as units_per_hour,
        
        -- Quality rate
        (excellent_shifts + good_shifts)::float / nullif(total_records, 0) * 100 as quality_shift_percentage
        
    from shift_aggregated
),

with_comparisons as (
    select
        *,
        
        -- Rankings
        rank() over (order by avg_oee desc) as oee_rank,
        rank() over (order by total_units_produced desc) as production_rank,
        rank() over (order by avg_defect_rate asc) as quality_rank,
        
        -- Comparison to best shift
        max(avg_oee) over () as best_shift_oee,
        avg_oee - max(avg_oee) over () as oee_gap_to_best,
        
        -- Improvement potential
        ((max(avg_oee) over () - avg_oee) / 100.0 * total_revenue) as potential_revenue_if_match_best,
        
        -- Performance score
        ((avg_oee / max(avg_oee) over ()) * 50 +
         (1 - avg_defect_rate / nullif(max(avg_defect_rate) over (), 0)) * 30 +
         (units_per_hour / max(units_per_hour) over ()) * 20) as performance_score
        
    from with_calcs
)

select
    -- Shift identification
    shift,
    
    -- Volume metrics
    total_records as shift_count,
    lines_operated,
    days_active,
    total_units_produced,
    total_units_target,
    target_achievement_percent,
    avg_units_per_shift,
    avg_units_per_day,
    units_per_hour,
    
    -- Performance metrics
    avg_oee,
    worst_oee,
    best_oee,
    oee_variability,
    
    -- Quality metrics
    total_defects,
    avg_defect_rate,
    overall_defect_rate,
    
    -- Efficiency metrics
    total_downtime_hours,
    avg_downtime_per_record,
    avg_downtime_per_day,
    avg_cycle_time,
    
    -- Financial metrics
    total_revenue,
    total_loss,
    total_defect_cost,
    total_downtime_cost,
    net_revenue,
    loss_percentage,
    
    -- Shift quality distribution
    excellent_shifts,
    good_shifts,
    poor_shifts,
    quality_shift_percentage,
    
    -- Rankings & comparisons
    oee_rank,
    production_rank,
    quality_rank,
    performance_score,
    
    -- Improvement potential
    oee_gap_to_best,
    potential_revenue_if_match_best,
    
    -- Recommendations
    case 
        when performance_score >= 90 then 'BENCHMARK - Use as training example'
        when performance_score >= 70 then 'GOOD - Minor optimizations possible'
        when performance_score >= 50 then 'REVIEW - Significant improvement needed'
        else 'CRITICAL - Immediate intervention required'
    end as recommendation,
    
    -- Metadata
    current_timestamp as mart_refreshed_at

from with_comparisons
order by performance_score desc
  );
  