with source as (
    select * from "analytics"."analytics"."iris_dataset"
)

select
    sepal_length,
    sepal_width,
    petal_length,
    petal_width,
    species
from source