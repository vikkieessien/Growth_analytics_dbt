with source as (
    select * from {{ source('marketing', 'ad_spend') }}
),

final as (
    select
        spend_id,
        campaign_id,
        date(spend_date)                as spend_date,
        cast(daily_spend as numeric)    as daily_spend,
        cast(impressions as int64)      as impressions,
        cast(clicks as int64)           as clicks,
        cast(conversions as int64)      as conversions,
        cast(cpc as numeric)            as cpc,
        cast(ctr as numeric)            as ctr
    from source
)

select * from final