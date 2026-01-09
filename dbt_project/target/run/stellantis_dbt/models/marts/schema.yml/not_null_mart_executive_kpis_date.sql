select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select date
from "stellantis_manufacturing"."dbt_dev_marts"."mart_executive_kpis"
where date is null



      
    ) dbt_internal_test