
    
    

with all_values as (

    select
        wear_status as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_prod_intermediate"."int_maintenance_features"
    group by wear_status

)

select *
from all_values
where value_field not in (
    'LOW','MODERATE','HIGH','CRITICAL'
)


