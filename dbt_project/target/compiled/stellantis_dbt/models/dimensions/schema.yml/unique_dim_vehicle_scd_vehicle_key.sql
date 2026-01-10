
    
    

select
    vehicle_key as unique_field,
    count(*) as n_records

<<<<<<< HEAD
from "stellantis_manufacturing"."dbt_dev_dimensions"."dim_vehicle_scd"
=======
from "stellantis_manufacturing"."dbt_dev"."dim_vehicle_scd"
>>>>>>> week4/day25-advanced-marts-scd
where vehicle_key is not null
group by vehicle_key
having count(*) > 1


