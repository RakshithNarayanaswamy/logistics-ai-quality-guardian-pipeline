with shipments as (
    select * from {{ ref('stg_shipments') }}
),

carriers as (
    select * from {{ ref('dim_carrier') }}
),

warehouses as (
    select * from {{ ref('dim_warehouse') }}
),

parts as (
    select * from {{ ref('dim_part') }}
)

select
    s.shipment_id,
    c.carrier_key,
    w.warehouse_key,
    p.part_key,
    s.order_date as order_date_key,
    s.shipment_date as shipment_date_key,
    s.promised_delivery_date as promised_delivery_date_key,
    s.actual_delivery_date as actual_delivery_date_key,

    s.status,
    s.quantity,
    s.cost,
    s.lead_time_days,
    s.is_on_time
from shipments s
left join carriers c on s.carrier_name = c.carrier_name
left join warehouses w on s.warehouse_id = w.warehouse_id
left join parts p on s.part_name = p.part_name
