select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select avg_oee
from "stellantis_manufacturing"."dbt_dev_marts"."mart_shift_analysis"
where avg_oee is null



      
    ) dbt_internal_test