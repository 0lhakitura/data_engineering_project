
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        species as value_field,
        count(*) as n_records

    from "analytics"."homework"."stg_iris"
    group by species

)

select *
from all_values
where value_field not in (
    'setosa','versicolor','virginica'
)



  
  
      
    ) dbt_internal_test