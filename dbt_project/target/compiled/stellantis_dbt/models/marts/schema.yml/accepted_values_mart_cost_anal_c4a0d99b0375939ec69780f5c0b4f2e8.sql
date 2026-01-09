
    
    

with all_values as (

    select
        cost_severity as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_dev_marts"."mart_cost_analysis"
    group by cost_severity

)

select *
from all_values
where value_field not in (
    'CRITICAL','HIGH','MEDIUM','LOW'
)


