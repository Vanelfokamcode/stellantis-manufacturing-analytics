
    
    

select
    production_line as unique_field,
    count(*) as n_records

from "stellantis_manufacturing"."dbt_prod_marts"."mart_line_performance"
where production_line is not null
group by production_line
having count(*) > 1


