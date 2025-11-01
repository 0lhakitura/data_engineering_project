



select
    1
from "analytics"."homework"."iris_processed"

where not((SELECT COUNT(*) FROM "analytics"."homework"."iris_processed") = 150)

