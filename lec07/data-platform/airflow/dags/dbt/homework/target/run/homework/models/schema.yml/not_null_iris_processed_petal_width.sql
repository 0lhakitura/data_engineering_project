
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select petal_width
from "analytics"."homework"."iris_processed"
where petal_width is null



  
  
      
    ) dbt_internal_test