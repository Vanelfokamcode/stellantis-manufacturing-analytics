select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select valid_from
<<<<<<< HEAD
from "stellantis_manufacturing"."dbt_dev_dimensions"."dim_vehicle_scd"
=======
from "stellantis_manufacturing"."dbt_dev"."dim_vehicle_scd"
>>>>>>> week4/day25-advanced-marts-scd
where valid_from is null



      
    ) dbt_internal_test