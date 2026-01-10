select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select total_units_produced
from "stellantis_manufacturing"."dbt_prod_marts"."mart_executive_kpis"
where total_units_produced is null



      
    ) dbt_internal_test