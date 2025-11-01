
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sepal_length
from "analytics"."homework"."stg_iris"
where sepal_length is null



  
  
      
    ) dbt_internal_test