{{ config(
    materialized='table',
    tags=['intermediate']
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

returns as (
    select * from {{ ref('stg_returns') }}
),

order_agg as (
    select
        customer_id,
        count(*) as total_orders,
        sum(total_cents) as lifetime_revenue_cents,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from orders
    group by customer_id
),

return_agg as (
    select
        o.customer_id,
        count(*) as total_returns,
        sum(r.refund_cents) as total_refund_cents
    from returns r
    join orders o on r.order_id = o.order_id
    group by o.customer_id
),

final as (
    select
        customers.customer_id,
        customers.full_name,
        customers.loyalty_tier,
        customers.created_at as customer_since,
        coalesce(order_agg.total_orders, 0) as total_orders,
        coalesce(order_agg.lifetime_revenue_cents, 0) as lifetime_revenue_cents,
        order_agg.first_order_date,
        order_agg.last_order_date,
        coalesce(return_agg.total_returns, 0) as total_returns,
        coalesce(return_agg.total_refund_cents, 0) as total_refund_cents
    from customers
    left join order_agg
        on customers.customer_id = order_agg.customer_id
    left join return_agg
        on customers.customer_id = return_agg.customer_id
)

select * from final
