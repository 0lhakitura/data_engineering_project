
  
    

  create  table "analytics"."analytics"."my_first_model__dbt_tmp"
  
  
    as
  
  (
    

SELECT 
  id,
  name,
  value,
  created_at,
  value * 2 as doubled_value
FROM 
  raw.example_data
  );
  