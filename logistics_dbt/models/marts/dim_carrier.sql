with distinct_carriers as (
    select distinct carrier_name
    from {{ ref('stg_shipments') }}
)

select
    md5(carrier_name) as carrier_key,
    carrier_name
from distinct_carriers
