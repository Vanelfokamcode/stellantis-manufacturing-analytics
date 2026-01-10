select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        cost_severity as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_prod_marts"."mart_cost_analysis"
    group by cost_severity

)

select *
from all_values
where value_field not in (
    'CRITICAL','HIGH','MEDIUM','LOW'
)



      
    ) dbt_internal_test