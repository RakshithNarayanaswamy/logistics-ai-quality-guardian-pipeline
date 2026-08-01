with source as (
    select * from {{ source('raw', 'inventory_snapshots') }}
)

select
    snapshot_date,
    part_name,
    warehouse_id,
    warehouse_inventory,
    inventory_cost_per_unit
from source
