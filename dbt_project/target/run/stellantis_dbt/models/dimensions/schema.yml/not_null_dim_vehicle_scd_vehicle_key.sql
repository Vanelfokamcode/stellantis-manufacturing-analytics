select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select vehicle_key
<<<<<<< HEAD
from "stellantis_manufacturing"."dbt_dev_dimensions"."dim_vehicle_scd"
=======
from "stellantis_manufacturing"."dbt_dev"."dim_vehicle_scd"
>>>>>>> week4/day25-advanced-marts-scd
where vehicle_key is null



      
    ) dbt_internal_test