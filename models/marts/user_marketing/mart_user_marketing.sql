with profiles as (
    select
        profile_id,
        first_activity_time,
        acquisition_source,
        signup_platform,
        signup_device_model,
        signup_country_code
    from {{ ref('stg_profiles') }}
    qualify row_number() over (        -- handles dupicated rows
        partition by profile_id
        order by first_activity_time asc
    ) = 1
),
total_revenue as (
    select
        profile_id,
        sum(gross_bookings_usd)  as total_revenue,
        count(*)                 as total_transactions    
    from {{ ref('stg_payments') }}
    group by 1
),
revenue_90d as (
   select
        profile_id,
        sum(gross_bookings_usd)                             as revenue_90d
    from {{ ref('int_payment_enriched') }}
    where date_diff(reported_date, install_date, day) 
          between 0 and 90                    --  use install_date directly
    group by 1
),
product_groups as (
    select
        profile_id,
        string_agg(distinct product_group order by product_group) as product_groups_used
    from {{ ref('stg_payments') }}
    group by 1
),
activity as (
    select
        profile_id,
        count(distinct activity_date)  as active_days,
        sum(songs_played)              as total_songs_played
    from {{ ref('stg_activity') }}
    group by 1
),

channel_metrics as (
    select * from {{ ref('int_channel_metrics') }}
),

final as (
    select
        p.profile_id,
        p.first_activity_time,
        p.acquisition_source                 as channel,
        cast(p.first_activity_time as date)                       as install_date,
        date_trunc(cast(p.first_activity_time as date), week)     as install_week,
        p.signup_platform,
        p.signup_device_model                as device_model,
        p.signup_country_code                as country_code,
        coalesce(a.active_days, 0)           as active_days,
        coalesce(a.total_songs_played, 0)    as total_songs_played,
        coalesce(t.total_revenue, 0)         as total_revenue,
        coalesce(t.total_transactions, 0)    as total_transactions,
        coalesce(r.revenue_90d, 0)           as revenue_90d,
        coalesce(pg.product_groups_used, 'none') as product_groups_used,
        case when coalesce(t.total_revenue, 0) > 0 then 1 else 0 end as is_payer,
        round(cm.cac_proxy, 2)                as cac_proxy,
       round(cm.clv_90d, 2)                  as clv_90d,
       round(cm.clv_arppu_x_retention, 2)    as clv_arppu_x_retention
    from profiles p
    left join total_revenue t   on p.profile_id = t.profile_id
    left join revenue_90d r     on p.profile_id = r.profile_id
    left join product_groups pg on p.profile_id = pg.profile_id
    left join activity a        on p.profile_id = a.profile_id
    left join channel_metrics cm on p.acquisition_source = cm.channel
)
select * from final