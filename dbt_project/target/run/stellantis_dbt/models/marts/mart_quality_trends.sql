
  
    

  create  table "stellantis_manufacturing"."dbt_prod_marts"."mart_quality_trends__dbt_tmp"
  
  
    as
  
  (
    -- models/marts/mart_quality_trends.sql
-- Quality trends analysis with anomaly detection
-- Answers: "Is quality improving? What are the patterns?"

with daily_quality as (
    select
        date,
        production_line,
        shift,
        
        -- Aggregated metrics
        sum(defects) as daily_defects,
        sum(units_produced) as daily_units,
        avg(defect_rate_percent) as avg_defect_rate,
        
        -- By category (from int_production_enriched)
        count(*) filter (where quality_category = 'EXCELLENT') as excellent_count,
        count(*) filter (where quality_category = 'GOOD') as good_count,
        count(*) filter (where quality_category = 'ACCEPTABLE') as acceptable_count,
        count(*) filter (where quality_category = 'POOR') as poor_count
        
    from "stellantis_manufacturing"."dbt_prod_intermediate"."int_production_enriched"
    group by date, production_line, shift
),

with_calculations as (
    select
        date,
        production_line,
        shift,
        daily_defects,
        daily_units,
        
        -- Defect rate
        (daily_defects::float / nullif(daily_units, 0) * 100) as defect_rate_percent,
        
        -- Quality score (0-100, higher = better)
        100 - (daily_defects::float / nullif(daily_units, 0) * 100) as quality_score,
        
        -- Category counts
        excellent_count,
        good_count,
        acceptable_count,
        poor_count,
        excellent_count + good_count + acceptable_count + poor_count as total_shifts,
        
        -- Rolling averages (7-day window)
        avg(daily_defects::float / nullif(daily_units, 0) * 100) over (
            partition by production_line
            order by date
            rows between 6 preceding and current row
        ) as rolling_7day_defect_rate,
        
        avg(daily_defects) over (
            partition by production_line
            order by date
            rows between 6 preceding and current row
        ) as rolling_7day_avg_defects,
        
        -- Month-to-date cumulative
        sum(daily_defects) over (
            partition by production_line, date_trunc('month', date)
            order by date
        ) as mtd_cumulative_defects,
        
        sum(daily_units) over (
            partition by production_line, date_trunc('month', date)
            order by date
        ) as mtd_cumulative_units
        
    from daily_quality
),

with_trends as (
    select
        *,
        
        -- Trend indicators
        defect_rate_percent - lag(defect_rate_percent) over (
            partition by production_line 
            order by date
        ) as defect_rate_change_vs_yesterday,
        
        defect_rate_percent - rolling_7day_defect_rate as deviation_from_7day_avg,
        
        -- MTD defect rate
        (mtd_cumulative_defects::float / nullif(mtd_cumulative_units, 0) * 100) as mtd_defect_rate,
        
        -- Anomaly detection (simple threshold-based)
        case 
            when abs(defect_rate_percent - rolling_7day_defect_rate) > 2.0 
            then true 
            else false 
        end as is_anomaly,
        
        -- Trend direction
        case 
            when defect_rate_percent > lag(defect_rate_percent, 3) over (
                partition by production_line order by date
            ) then 'WORSENING'
            when defect_rate_percent < lag(defect_rate_percent, 3) over (
                partition by production_line order by date
            ) then 'IMPROVING'
            else 'STABLE'
        end as trend_direction,
        
        -- Quality consistency (lower std dev = more consistent)
        stddev(defect_rate_percent) over (
            partition by production_line
            order by date
            rows between 6 preceding and current row
        ) as quality_variability_7day
        
    from with_calculations
)

select
    -- Identifiers
    date,
    production_line,
    shift,
    
    -- Volume
    daily_units,
    daily_defects,
    
    -- Quality metrics
    defect_rate_percent,
    quality_score,
    
    -- Rolling metrics
    rolling_7day_defect_rate,
    rolling_7day_avg_defects,
    quality_variability_7day,
    
    -- MTD metrics
    mtd_cumulative_defects,
    mtd_cumulative_units,
    mtd_defect_rate,
    
    -- Trends
    defect_rate_change_vs_yesterday,
    deviation_from_7day_avg,
    trend_direction,
    
    -- Anomaly flag
    is_anomaly,
    
    -- Category distribution
    excellent_count,
    good_count,
    acceptable_count,
    poor_count,
    total_shifts,
    (excellent_count::float / nullif(total_shifts, 0) * 100) as excellent_percentage,
    
    -- Alert level
    case 
        when defect_rate_percent > 5.0 and trend_direction = 'WORSENING' 
            then 'CRITICAL - Immediate Action'
        when defect_rate_percent > 3.0 and trend_direction = 'WORSENING'
            then 'HIGH - Review Required'
        when is_anomaly = true
            then 'MEDIUM - Investigate Anomaly'
        else 'LOW - Normal Operations'
    end as alert_level,
    
    -- Metadata
    current_timestamp as mart_refreshed_at

from with_trends
order by date desc, production_line, shift
  );
  