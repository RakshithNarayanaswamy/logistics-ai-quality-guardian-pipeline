-- Union across every fact source that references a part, not just shipments —
-- otherwise fact_inventory_snapshot's parts (which may not all have shipped yet)
-- would fail their relationships test against this dimension.
with distinct_parts as (
    select part_name from {{ ref('stg_shipments') }}
    union
    select part_name from {{ ref('stg_inventory_snapshots') }}
)

select distinct
    md5(part_name) as part_key,
    part_name
from distinct_parts
