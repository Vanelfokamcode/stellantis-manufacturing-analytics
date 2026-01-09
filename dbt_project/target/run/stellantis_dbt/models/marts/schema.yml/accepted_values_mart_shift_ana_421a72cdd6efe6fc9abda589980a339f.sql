select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        recommendation as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_dev_marts"."mart_shift_analysis"
    group by recommendation

)

select *
from all_values
where value_field not in (
    'BENCHMARK - Use as training example','GOOD - Minor optimizations possible','REVIEW - Significant improvement needed','CRITICAL - Immediate intervention required'
)



      
    ) dbt_internal_test