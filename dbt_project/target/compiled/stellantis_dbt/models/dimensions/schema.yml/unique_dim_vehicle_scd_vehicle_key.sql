
    
    

select
    vehicle_key as unique_field,
    count(*) as n_records

from "stellantis_manufacturing"."dbt_dev_dimensions"."dim_vehicle_scd"
where vehicle_key is not null
group by vehicle_key
having count(*) > 1


