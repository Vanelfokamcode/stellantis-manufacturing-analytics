select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select oee_percent
from "stellantis_manufacturing"."dbt_dev_intermediate"."int_production_enriched"
where oee_percent is null



      
    ) dbt_internal_test