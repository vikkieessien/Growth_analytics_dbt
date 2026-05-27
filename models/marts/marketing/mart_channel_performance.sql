with final as (
    select
        acquisition_channel,
        total_customers,
        total_revenue,
        avg_ltv,
        active_subscribers,
        churned_customers,
        total_spend,
        cac,
        ltv_cac_ratio,
        round(churned_customers / nullif(total_customers, 0), 2) as churn_rate,
        round(active_subscribers / nullif(total_customers, 0), 2) as activation_rate,
        round(total_revenue / nullif(total_spend, 0), 2 ) as revenue_per_spend

from {{ref('int_channel_ltv_cac')}}
)

select * from final 