
    
    

with all_values as (

    select
        primary_bottleneck as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_dev_intermediate"."int_oee_breakdown"
    group by primary_bottleneck

)

select *
from all_values
where value_field not in (
    'AVAILABILITY','PERFORMANCE','QUALITY','NONE'
)


