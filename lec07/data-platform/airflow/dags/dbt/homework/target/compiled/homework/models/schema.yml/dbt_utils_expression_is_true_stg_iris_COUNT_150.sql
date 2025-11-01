



select
    1
from "analytics"."homework"."stg_iris"

where not(COUNT(*) = 150)

