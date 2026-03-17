with users_activity as (
    select * from {{ source('marketing_raw', 'Activity') }}
),
final as (
    select
        cast(profile_id as string)  as profile_id,
        cast(activity_date as date) as activity_date,
        cast(songs_played as int64) as songs_played
    from users_activity
)
select * from final