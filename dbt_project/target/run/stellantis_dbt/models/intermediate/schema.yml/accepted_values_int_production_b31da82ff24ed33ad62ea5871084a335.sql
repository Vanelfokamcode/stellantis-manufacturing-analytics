select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        oee_category as value_field,
        count(*) as n_records

    from "stellantis_manufacturing"."dbt_prod_intermediate"."int_production_enriched"
    group by oee_category

)

select *
from all_values
where value_field not in (
    'EXCELLENT','GOOD','ACCEPTABLE','NEEDS_ATTENTION'
)



      
    ) dbt_internal_test