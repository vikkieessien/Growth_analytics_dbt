with payments as (
    select * from {{ ref('stg_payments') }}
),

profiles as (
    select
        profile_id,
        cast(first_activity_time as date)  as install_date,
        signup_platform,
        signup_device_model                as device_model,
        signup_country_code                as country_code,
        acquisition_source                 as channel
    from {{ ref('stg_profiles') }}
    qualify row_number() over (
        partition by profile_id
        order by first_activity_time asc
    ) = 1
),

final as (
    select
        pr.profile_id,
        pr.install_date,
        pr.signup_platform,
        pr.device_model,
        pr.country_code,
        pr.channel,
        p.reported_date,
        p.product_group,
        p.gross_bookings_usd,
        date_diff(p.reported_date, pr.install_date, day) as days_since_install
    from payments p
    left join profiles pr
        on p.profile_id = pr.profile_id
)

select * from final