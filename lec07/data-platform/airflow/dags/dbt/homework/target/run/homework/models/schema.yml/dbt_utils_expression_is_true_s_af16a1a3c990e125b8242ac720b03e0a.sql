
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "analytics"."homework"."stg_iris"

where not((SELECT COUNT(*) FROM "analytics"."homework"."stg_iris") = 150)


  
  
      
    ) dbt_internal_test