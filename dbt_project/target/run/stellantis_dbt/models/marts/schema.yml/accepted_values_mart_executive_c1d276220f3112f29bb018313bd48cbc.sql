select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        factory_health_status as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_prod_marts"."mart_executive_kpis"
    group by factory_health_status

)

select *
from all_values
where value_field not in (
    'EXCELLENT','GOOD','ACCEPTABLE','CRITICAL'
)



      
    ) dbt_internal_test