
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sepal_width
from "analytics"."homework"."iris_processed"
where sepal_width is null



  
  
      
    ) dbt_internal_test