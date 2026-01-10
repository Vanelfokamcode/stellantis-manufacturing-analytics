select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select date
from "stellantis_manufacturing"."dbt_prod_marts"."mart_cost_analysis"
where date is null



      
    ) dbt_internal_test