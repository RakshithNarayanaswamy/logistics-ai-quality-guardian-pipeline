with snapshots as (
    select * from {{ ref('stg_inventory_snapshots') }}
),

parts as (
    select * from {{ ref('dim_part') }}
),

warehouses as (
    select * from {{ ref('dim_warehouse') }}
)

select
    s.snapshot_date,
    p.part_key,
    w.warehouse_key,
    s.warehouse_inventory,
    s.inventory_cost_per_unit
from snapshots s
left join parts p on s.part_name = p.part_name
left join warehouses w on s.warehouse_id = w.warehouse_id
