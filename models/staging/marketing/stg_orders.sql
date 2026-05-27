with source as (
    select * from {{ source('marketing', 'orders') }}
),

final as (
    select
        order_id,
        customer_id,
        product,
        date(order_date)                                            as order_date,
        cast(amount as numeric)                                     as order_amount,
        cast(refund_amount as numeric)                              as refund_amount,
        cast(amount as numeric) - cast(refund_amount as numeric)    as net_amount,
        status                                                      as order_status,
        payment_method
    from source
)

select * from final