
    
    

with all_values as (

    select
        factory_health_status as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_dev_marts"."mart_executive_kpis"
    group by factory_health_status

)

select *
from all_values
where value_field not in (
    'EXCELLENT','GOOD','ACCEPTABLE','CRITICAL'
)


