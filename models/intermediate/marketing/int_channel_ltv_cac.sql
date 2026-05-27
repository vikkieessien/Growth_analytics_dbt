with customer_metrics as (
    select
        acquisition_channel,
        count(distinct customer_id)                 as total_customers,
        round(sum(net_amount), 2)                   as total_revenue,
        round(avg(net_amount), 2)                   as avg_ltv,
        sum(case 
              when subscription_status = 'active' 
               then 1 else 0 end)   as active_subscribers,
        sum(case
               when subscription_status = 'cancelled'
                then 1 else 0 end)  as churned_customers
    from {{ref('int_customer_acquisition')}}
    group by acquisition_channel
),

spend_metrics as ( 
    select 
        channel,
        round(sum(marketing_spend),2) as total_spend

    from {{ref('int_campaign_performance')}}
    group by 
          channel

),

final as (
    select
        c.acquisition_channel,
        c.total_customers,
        c.total_revenue,
        c.avg_ltv,
        c.active_subscribers,
        c.churned_customers,
        s.total_spend,
        round(s.total_spend / nullif(c.total_customers, 0), 2)          as cac,
        round(c.avg_ltv / nullif(s.total_spend / nullif(c.total_customers, 0), 0), 2) as ltv_cac_ratio
    from customer_metrics c
    left join spend_metrics s
        on c.acquisition_channel = s.channel
)

select * from final