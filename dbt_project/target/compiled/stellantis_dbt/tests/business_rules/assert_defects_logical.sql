-- tests/business_rules/assert_defects_logical.sql
-- Business rule: Defects cannot be greater than units produced
-- This would be logically impossible

select
    production_line,
    date,
    shift,
    units_produced,
    defects,
    'Defects exceed production count' as failure_reason
from "stellantis_manufacturing"."dbt_prod_intermediate"."int_production_enriched"
where defects > units_produced