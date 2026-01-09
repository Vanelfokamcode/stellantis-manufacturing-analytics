
    
    

with all_values as (

    select
        quality_status as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_dev_intermediate"."int_quality_aggregated"
    group by quality_status

)

select *
from all_values
where value_field not in (
    'PASS','FAIL','UNKNOWN'
)


