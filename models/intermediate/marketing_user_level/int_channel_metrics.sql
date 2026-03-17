with user_base as (
    select
        p.acquisition_source                               as channel,
        count(distinct p.profile_id)                       as total_installs,
        count(distinct case
            when coalesce(t.gross_bookings_usd, 0) > 0
            then p.profile_id end)                         as total_payers,
        coalesce(sum(t.gross_bookings_usd), 0)             as channel_total_revenue,
        coalesce(sum(case
            when date_diff(
                t.reported_date,
                cast(p.first_activity_time as date), day)
            between 0 and 90
            then t.gross_bookings_usd end), 0)             as channel_revenue_90d

    from {{ ref('stg_profiles') }} p
    left join {{ ref('stg_payments') }} t
        on p.profile_id = t.profile_id
    group by 1
),

final as (
    select
        channel,
        total_installs,
        total_payers,
        channel_total_revenue,
        channel_revenue_90d,
        safe_divide(channel_total_revenue, total_payers)   as cac_proxy,
        safe_divide(channel_revenue_90d, total_payers)     as clv_90d,
        safe_divide(channel_total_revenue, total_installs) as clv_arppu_x_retention
    from user_base
)

select * from final