select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        risk_category as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_prod_marts"."mart_maintenance_overview"
    group by risk_category

)

select *
from all_values
where value_field not in (
    'CRITICAL','HIGH','MEDIUM','LOW'
)



      
    ) dbt_internal_test