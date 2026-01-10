select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select is_current
from "stellantis_manufacturing"."dbt_dev"."dim_vehicle_scd"
where is_current is null



      
    ) dbt_internal_test