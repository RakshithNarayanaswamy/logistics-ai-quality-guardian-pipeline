with distinct_warehouses as (
    select distinct warehouse_id
    from {{ ref('stg_shipments') }}
)

select
    md5(warehouse_id) as warehouse_key,
    warehouse_id
from distinct_warehouses
