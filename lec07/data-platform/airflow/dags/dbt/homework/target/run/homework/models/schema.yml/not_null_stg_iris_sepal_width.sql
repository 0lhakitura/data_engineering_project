
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sepal_width
from "analytics"."homework"."stg_iris"
where sepal_width is null



  
  
      
    ) dbt_internal_test