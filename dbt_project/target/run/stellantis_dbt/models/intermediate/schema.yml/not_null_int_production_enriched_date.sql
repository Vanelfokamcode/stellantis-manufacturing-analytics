select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select date
from "stellantis_manufacturing"."dbt_prod_intermediate"."int_production_enriched"
where date is null



      
    ) dbt_internal_test