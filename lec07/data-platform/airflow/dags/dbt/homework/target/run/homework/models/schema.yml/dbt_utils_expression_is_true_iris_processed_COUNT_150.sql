
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "analytics"."homework"."iris_processed"

where not(COUNT(*) = 150)


  
  
      
    ) dbt_internal_test