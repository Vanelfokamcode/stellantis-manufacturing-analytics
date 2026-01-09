select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select valid_from
from "stellantis_manufacturing"."dbt_dev_dimensions"."dim_vehicle_scd"
where valid_from is null



      
    ) dbt_internal_test