



select
    1
from "analytics"."homework"."stg_iris"

where not((SELECT COUNT(*) FROM "analytics"."homework"."stg_iris") = 150)

