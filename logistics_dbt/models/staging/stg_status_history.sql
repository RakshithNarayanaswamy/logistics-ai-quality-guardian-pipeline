with source as (
    select * from {{ source('raw', 'shipment_status_history') }}
)

select
    shipment_id,
    event_status,
    event_ts,
    event_sequence
from source
