select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select unit_device_id
from "stellantis_manufacturing"."dbt_dev_intermediate"."int_maintenance_features"
where unit_device_id is null



      
    ) dbt_internal_test