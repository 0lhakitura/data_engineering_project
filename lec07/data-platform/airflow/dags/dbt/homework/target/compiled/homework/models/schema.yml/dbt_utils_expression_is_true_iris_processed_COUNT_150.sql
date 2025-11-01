



select
    1
from "analytics"."homework"."iris_processed"

where not(COUNT(*) = 150)

