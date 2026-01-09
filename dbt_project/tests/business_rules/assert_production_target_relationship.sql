-- tests/business_rules/assert_production_target_relationship.sql
-- Business rule: Units produced shouldn't be >150% of target
-- Extreme overproduction indicates possible data error

select
    production_line,
    date,
    shift,
    units_produced,
    units_target,
    (units_produced::float / nullif(units_target, 0) * 100) as achievement_percent,
    'Production exceeds 150% of target - possible error' as failure_reason
from {{ ref('int_production_enriched') }}
where units_produced > (units_target * 1.5)
