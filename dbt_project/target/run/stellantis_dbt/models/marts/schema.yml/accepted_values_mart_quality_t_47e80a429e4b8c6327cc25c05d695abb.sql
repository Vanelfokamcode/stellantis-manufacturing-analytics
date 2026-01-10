select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        trend_direction as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_prod_marts"."mart_quality_trends"
    group by trend_direction

)

select *
from all_values
where value_field not in (
    'IMPROVING','STABLE','WORSENING'
)



      
    ) dbt_internal_test