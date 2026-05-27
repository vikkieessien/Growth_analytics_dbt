with source as (
    select * from {{ source('marketing', 'customers') }}
),

final as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        acquisition_channel,
        plan_type,
        country,
        date(signup_date)   as signup_date,
        age,
        is_active
    from source
)

select * from final