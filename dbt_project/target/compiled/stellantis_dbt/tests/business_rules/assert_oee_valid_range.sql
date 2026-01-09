-- tests/business_rules/assert_oee_valid_range.sql
-- Business rule: OEE must be between 0 and 100 percent
-- Any value outside this range indicates data quality issue

select
    production_line,
    date,
    shift,
    oee_percent,
    'OEE out of valid range (0-100)' as failure_reason
from "stellantis_manufacturing"."dbt_dev_intermediate"."int_production_enriched"
where oee_percent < 0 
   or oee_percent > 100