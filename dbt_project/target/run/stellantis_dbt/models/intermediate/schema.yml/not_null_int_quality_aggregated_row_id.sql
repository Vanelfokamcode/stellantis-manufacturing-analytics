select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select row_id
from "stellantis_manufacturing"."dbt_dev_intermediate"."int_quality_aggregated"
where row_id is null



      
    ) dbt_internal_test