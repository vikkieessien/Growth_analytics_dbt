with final as (
    select
        customer_id,
        signup_date,
        first_order_date,
        last_order_date,
        first_name,
        last_name,
        email,
        country,
        acquisition_channel,
        plan_type,
        total_orders,
        total_revenue,
        refunds,
        net_amount,
        subscription_plan,
        subscription_status,
        billing_cycle,
        subscription_amount,
        subscription_start_date,
        subscription_end_date,
        case
            when net_amount > 50 then 'high_value'
            when net_amount between 20 and 50 then 'mid_value'
            when net_amount < 20 then 'low_value'
        end as value_segment,
        case
            when subscription_status in ('cancelled', 'churned') then 'high_risk'
            when subscription_status = 'active' then 'low_risk'
            else 'unknown'
        end as churn_risk
    from {{ ref('int_customer_acquisition') }}
)

select * from final