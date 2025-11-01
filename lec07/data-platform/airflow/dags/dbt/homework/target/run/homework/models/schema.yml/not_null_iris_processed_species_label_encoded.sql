
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select species_label_encoded
from "analytics"."homework"."iris_processed"
where species_label_encoded is null



  
  
      
    ) dbt_internal_test