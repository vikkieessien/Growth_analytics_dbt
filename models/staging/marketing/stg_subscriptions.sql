with source as (
    select * from {{ source('marketing', 'subscriptions') }}
),

final as (
    select
        subscription_id,
        customer_id,
        product_id,
        plan_type,
        billing_cycle,
        cast(amount as numeric)         as subscription_amount,
        date(start_date)                as start_date,
        case
        when end_date is null then null
        else end_date
        end                              as end_date,
        status,
        cast(trial_used as bool)        as trial_used,
        cast(auto_renew as bool)        as auto_renew,
        nullif(cancellation_reason, '') as cancellation_reason
    from source
)

select * from final