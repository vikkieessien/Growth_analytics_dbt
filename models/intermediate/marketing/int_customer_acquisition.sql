with user_level as (
select 
    c.customer_id,
    c.signup_date,
    c.first_name,
    c.last_name,
    c.email,
    c.country,
    c.acquisition_channel,
    c.plan_type,

    
    
   count(distinct r.order_id) as total_orders,
    sum(r.order_amount) as total_revenue,
    sum(r.refund_amount) as refunds,
    sum(r.net_amount) as net_amount,
    min(r.order_date)           as first_order_date,
    max(r.order_date)           as last_order_date
    
  from {{ref('stg_customers')}} c
  left join {{ref('stg_orders')}} r
  on c.customer_id = r.customer_id
  group by
    c.customer_id,
    c.signup_date,
    c.first_name,
    c.last_name,
    c.email,
    c.country,
    c.acquisition_channel,
    c.plan_type
    
),

latest_subscription as (
    select
        customer_id,
        plan_type as subscription_plan,
        status as subscription_status,
        billing_cycle,
        subscription_amount,
        start_date as subscription_start_date,
        end_date as subscription_end_date
    from {{ref('stg_subscriptions')}}
    qualify row_number() over (
        partition by customer_id
        order by start_date desc
    ) = 1
),

final as (

select 
    c.customer_id,
    c.signup_date,
    c.first_order_date,
    c.last_order_date,
    c.first_name,
    c.last_name,
    c.email,
    c.country,
    c.acquisition_channel,
    c.plan_type,
    c.total_orders,
    round(c.total_revenue, 2) as total_revenue,
    round(c.refunds,2) as refunds,
    net_amount,
   s.subscription_plan,
    s.subscription_status,
    s.billing_cycle,
    s.subscription_amount,
    s.subscription_start_date,
    s.subscription_end_date

from user_level c
left join latest_subscription s
on c.customer_id = s.customer_id

)

select * from final