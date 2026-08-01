select
    shipment_id,
    event_status,
    event_ts,
    event_sequence
from {{ ref('stg_status_history') }}
