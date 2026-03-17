with profiles as (
    select
        profile_id,
        acquisition_source,
        signup_platform,
        date_trunc(cast(first_activity_time as date), week) as install_week
    from {{ ref('stg_profiles') }}
),
activity_enriched as (
    select
        a.profile_id,
        a.activity_date,
        date_trunc(a.activity_date, week)  as activity_week,
        p.install_week,
        p.signup_platform,
        date_diff(
            date_trunc(a.activity_date, week),
            p.install_week, week
        ) as weeks_since_install
    from {{ ref('stg_activity') }} a
    left join profiles p on a.profile_id = p.profile_id
),
activity_windowed as (
    select * from activity_enriched
    where weeks_since_install between 0 and 12
),
cohort_size as (
    select
        install_week,
        signup_platform,
        count(distinct profile_id) as cohort_size
    from profiles
    group by 1, 2
),
retained as (
    select
        install_week,
        weeks_since_install,
        signup_platform,
        count(distinct profile_id) as retained_users
    from activity_windowed
    group by 1, 2, 3
),
payments_enriched as (
    select
        pay.profile_id,
        pay.gross_bookings_usd,
        date_trunc(pay.reported_date, week)     as payment_week,
        p.install_week,
        p.signup_platform,
        date_diff(
            date_trunc(pay.reported_date, week),
            p.install_week, week
        )                                       as weeks_since_install
    from {{ ref('stg_payments') }} pay       
    left join profiles p 
        on pay.profile_id = p.profile_id
),
weekly_revenue as (
    select
        install_week,
        weeks_since_install,
        signup_platform,
        sum(gross_bookings_usd) as weekly_revenue
    from payments_enriched
    where weeks_since_install between 0 and 12
    group by 1, 2, 3
),
final as (
    select
        r.install_week,
        r.weeks_since_install,
        r.signup_platform,
        r.retained_users,
        cs.cohort_size,
        safe_divide(r.retained_users, cs.cohort_size)          as retention_rate, -- bigqQuery function nullif
        coalesce(wr.weekly_revenue, 0)                         as weekly_revenue,
        safe_divide(coalesce(wr.weekly_revenue, 0), r.retained_users) as revenue_per_retained_user
    from retained r
    left join cohort_size cs
        on r.install_week    = cs.install_week
       and r.signup_platform = cs.signup_platform
    left join weekly_revenue wr
        on r.install_week        = wr.install_week
       and r.weeks_since_install = wr.weeks_since_install
       and r.signup_platform     = wr.signup_platform
)
select * from final
order by install_week, weeks_since_install, signup_platform