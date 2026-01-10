
    
    

select
    shift as unique_field,
    count(*) as n_records

from "stellantis_manufacturing"."dbt_prod_marts"."mart_shift_analysis"
where shift is not null
group by shift
having count(*) > 1


