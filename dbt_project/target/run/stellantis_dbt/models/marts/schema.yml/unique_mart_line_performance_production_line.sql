select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    production_line as unique_field,
    count(*) as n_records

from "stellantis_manufacturing"."dbt_dev_marts"."mart_line_performance"
where production_line is not null
group by production_line
having count(*) > 1



      
    ) dbt_internal_test