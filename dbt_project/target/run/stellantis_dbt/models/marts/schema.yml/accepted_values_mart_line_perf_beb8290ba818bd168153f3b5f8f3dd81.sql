select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        priority_level as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_dev_marts"."mart_line_performance"
    group by priority_level

)

select *
from all_values
where value_field not in (
    'CRITICAL - Immediate Action','HIGH - Schedule Review','MEDIUM - Monitor Closely','LOW - Maintain Performance'
)



      
    ) dbt_internal_test