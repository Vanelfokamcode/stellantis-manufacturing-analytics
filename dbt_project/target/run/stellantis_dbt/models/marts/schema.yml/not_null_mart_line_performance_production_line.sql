select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select production_line
from "stellantis_manufacturing"."dbt_dev_marts"."mart_line_performance"
where production_line is null



      
    ) dbt_internal_test