
    
    

select
    row_id as unique_field,
    count(*) as n_records

from "stellantis_manufacturing"."dbt_prod_intermediate"."int_quality_aggregated"
where row_id is not null
group by row_id
having count(*) > 1


