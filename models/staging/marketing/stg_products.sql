with source as (
    select * from {{ source('marketing', 'products') }}
),

final as (
    select
        product_id,
        product_name,
        cast(price_monthly as numeric)  as price_monthly,
        cast(price_annual as numeric)   as price_annual,
        features,
        cast(max_users as int64)        as max_users,
        is_active
    from source
)

select * from final