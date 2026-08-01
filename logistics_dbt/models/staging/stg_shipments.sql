with source as (
    select * from {{ source('raw', 'shipments') }}
),

renamed as (
    select
        shipment_id,
        part_name,
        carrier_name,
        warehouse_id,
        status,
        quantity,
        cost,
        order_date,
        shipment_date,
        promised_delivery_date,
        actual_delivery_date,

        -- OTIF: delivered on/before the promised date
        case
            when status = 'delivered'
                then actual_delivery_date <= promised_delivery_date
        end as is_on_time,

        datediff('day', order_date, actual_delivery_date) as lead_time_days
    from source
)

select * from renamed
