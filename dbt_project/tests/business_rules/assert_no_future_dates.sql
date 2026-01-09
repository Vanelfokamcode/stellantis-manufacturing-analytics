-- tests/business_rules/assert_no_future_dates.sql
-- Business rule: Production dates cannot be in the future
-- Future dates indicate data entry errors

select
    production_line,
    date,
    shift,
    'Future production date detected' as failure_reason
from {{ ref('int_production_enriched') }}
where date > current_date
