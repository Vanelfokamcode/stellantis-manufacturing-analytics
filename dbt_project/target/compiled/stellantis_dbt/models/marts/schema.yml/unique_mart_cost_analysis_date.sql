
    
    

select
    date as unique_field,
    count(*) as n_records

from "stellantis_manufacturing"."dbt_prod_marts"."mart_cost_analysis"
where date is not null
group by date
having count(*) > 1


