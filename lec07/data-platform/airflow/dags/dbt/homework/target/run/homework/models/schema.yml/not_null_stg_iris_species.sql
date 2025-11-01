
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select species
from "analytics"."homework"."stg_iris"
where species is null



  
  
      
    ) dbt_internal_test