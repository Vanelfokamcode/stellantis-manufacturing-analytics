select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        wear_status as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_dev_intermediate"."int_maintenance_features"
    group by wear_status

)

select *
from all_values
where value_field not in (
    'LOW','MODERATE','HIGH','CRITICAL'
)



      
    ) dbt_internal_test