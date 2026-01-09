select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- tests/business_rules/assert_downtime_reasonable.sql
-- Business rule: Downtime cannot exceed 8 hours (one shift)
-- Values above 8 hours indicate data errors

select
    production_line,
    date,
    shift,
    downtime_hours,
    'Downtime exceeds shift duration' as failure_reason
from "stellantis_manufacturing"."dbt_dev_intermediate"."int_production_enriched"
where downtime_hours > 8.0
      
    ) dbt_internal_test