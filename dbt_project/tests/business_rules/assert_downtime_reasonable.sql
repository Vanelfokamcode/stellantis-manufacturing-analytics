-- tests/business_rules/assert_downtime_reasonable.sql
-- Business rule: Downtime cannot exceed 8 hours (one shift)
-- Values above 8 hours indicate data errors

select
    production_line,
    date,
    shift,
    downtime_hours,
    'Downtime exceeds shift duration' as failure_reason
from {{ ref('int_production_enriched') }}
where downtime_hours > 8.0
