{{ config(
    materialized='table',
    schema='marts_pii',
    tags=['marts', 'pii', 'restricted'],
    meta={
        'contains_pii': true,
        'owner': 'data-governance',
        'compliance': 'CCPA / GDPR'
    }
) }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_lifetime as (
    select * from {{ ref('int_customer_lifetime') }}
),

final as (
    select
        customers.customer_id,
        customers.full_name,
        customers.email,
        customers.phone,
        customers.shipping_address,
        customers.billing_address,
        customers.loyalty_tier,
        customers.created_at,
        coalesce(customer_lifetime.total_orders, 0) as total_orders,
        coalesce(customer_lifetime.lifetime_revenue_cents, 0) as lifetime_revenue_cents,
        customer_lifetime.first_order_date,
        customer_lifetime.last_order_date,
        coalesce(customer_lifetime.total_returns, 0) as total_returns,
        coalesce(customer_lifetime.total_refund_cents, 0) as total_refund_cents
    from customers
    left join customer_lifetime
        on customers.customer_id = customer_lifetime.customer_id
)

select * from final
