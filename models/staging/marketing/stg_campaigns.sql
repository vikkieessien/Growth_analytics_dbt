with source as (
    select * from {{ source('marketing', 'campaigns') }}
),

final as (
    select
        campaign_id,
        campaign_name,
        channel,
        campaign_type,
        date(start_date)        as start_date,
        date(end_date)          as end_date,
        cast(budget as numeric) as budget,
        target_country,
        status
    from source
)

select * from final