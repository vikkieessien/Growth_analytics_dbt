-- models/staging/stg_profiles.sql
with profiles_data as (
    select * from {{ source('marketing_raw', 'profiles') }}
),
final as (
    select
        cast(profile_id as string)             as profile_id,
        cast(first_activity_time as timestamp) as first_activity_time,
        signup_platform,
        signup_device_model,
        signup_country_code,
        acquisition_source
    from profiles_data
)
select * from final