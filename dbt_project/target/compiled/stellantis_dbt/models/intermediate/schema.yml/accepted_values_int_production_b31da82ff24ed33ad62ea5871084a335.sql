
    
    

with all_values as (

    select
        oee_category as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_dev_intermediate"."int_production_enriched"
    group by oee_category

)

select *
from all_values
where value_field not in (
    'EXCELLENT','GOOD','ACCEPTABLE','NEEDS_ATTENTION'
)


