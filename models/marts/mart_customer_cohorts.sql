{{ config(
    materialized='table',
    tags=['marts']
) }}

with customer_lifetime as (
    select * from {{ ref('int_customer_lifetime') }}
),

final as (
    select
        customer_id,
        full_name,
        loyalty_tier,
        customer_since,
        total_orders,
        lifetime_revenue_cents,
        first_order_date,
        last_order_date,
        total_returns,
        total_refund_cents,
        case
            when total_orders = 0 then 'no_purchase'
            when total_orders = 1 then 'one_time'
            when total_orders between 2 and 5 then 'repeat'
            else 'loyal'
        end as customer_segment,
        case
            when lifetime_revenue_cents > 50000 then 'high_value'
            when lifetime_revenue_cents > 10000 then 'mid_value'
            else 'low_value'
        end as value_tier
    from customer_lifetime
)

select * from final
