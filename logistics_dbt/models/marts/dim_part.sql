with distinct_parts as (
    select distinct part_name
    from {{ ref('stg_shipments') }}
)

select
    md5(part_name) as part_key,
    part_name
from distinct_parts
