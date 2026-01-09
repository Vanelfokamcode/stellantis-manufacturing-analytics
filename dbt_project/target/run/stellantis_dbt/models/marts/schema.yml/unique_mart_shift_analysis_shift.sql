select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    shift as unique_field,
    count(*) as n_records

from "stellantis_manufacturing"."dbt_dev_marts"."mart_shift_analysis"
where shift is not null
group by shift
having count(*) > 1



      
    ) dbt_internal_test