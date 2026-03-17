with payment_data as (
    select * from {{ source('marketing_raw', 'Payments') }}
),
final as (
    select
        cast(profile_id as string)          as profile_id,
        cast(reported_date as date)         as reported_date,
        product_group,
        cast(gross_bookings_usd as float64) as gross_bookings_usd
    from payment_data
    where gross_bookings_usd is not null
      and cast(gross_bookings_usd as float64) > 0
)
select * from final