select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select shift
from "stellantis_manufacturing"."dbt_prod_marts"."mart_shift_analysis"
where shift is null



      
    ) dbt_internal_test