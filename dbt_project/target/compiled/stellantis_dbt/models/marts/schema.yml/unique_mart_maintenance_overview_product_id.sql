
    
    

select
    product_id as unique_field,
    count(*) as n_records

from "stellantis_manufacturing"."dbt_dev_marts"."mart_maintenance_overview"
where product_id is not null
group by product_id
having count(*) > 1


