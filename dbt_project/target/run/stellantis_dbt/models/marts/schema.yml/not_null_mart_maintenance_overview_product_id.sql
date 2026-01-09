select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select product_id
from "stellantis_manufacturing"."dbt_dev_marts"."mart_maintenance_overview"
where product_id is null



      
    ) dbt_internal_test