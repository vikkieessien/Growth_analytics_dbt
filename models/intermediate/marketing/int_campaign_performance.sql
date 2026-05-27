with campaigns_data as (
select 
    campaign_id,
    campaign_name,
    channel,
    campaign_type,
    start_date as campaign_start_date,
    end_date as campaign_end_date,
    target_country,
    status as campaign_status,
    budget as campaign_budget
    
from {{ref('stg_campaigns')}}
),

spend_data  as (
  select
    campaign_id,
    sum(daily_spend) as marketing_spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(conversions) as conversions,
    round(safe_divide(sum(daily_spend), sum(clicks)),2)      as cpc,
    round(safe_divide(sum(clicks), sum(impressions)),2)      as ctr,
    round(safe_divide(sum(daily_spend), sum(conversions)),2) as cpa

from {{ref('stg_ad_spend')}}
group by
    campaign_id
    

),
final as (

select 
    cp.campaign_id,
    cp.campaign_name,
    cp.channel,
    cp.campaign_type,
    cp.campaign_start_date,
    cp.campaign_end_date,
    cp.target_country,
    cp.campaign_status,
    cp.campaign_budget,
    sp.marketing_spend,
    sp.impressions,
    sp.clicks,
    sp.conversions,
    sp.cpc,
    sp.ctr,
    sp.cpa

  from campaigns_data cp
  left join spend_data sp
  on cp.campaign_id = sp.campaign_id
    
)

select * from final