select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        primary_cost_driver as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_dev_marts"."mart_cost_analysis"
    group by primary_cost_driver

)

select *
from all_values
where value_field not in (
    'DEFECTS','DOWNTIME','BALANCED'
)



      
    ) dbt_internal_test