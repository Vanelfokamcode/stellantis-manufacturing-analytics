select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select vehicle_key
from "stellantis_manufacturing"."dbt_prod"."dim_vehicle_scd"
where vehicle_key is null



      
    ) dbt_internal_test