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

order_items as (
    select
        order_id,
        count(*) as item_count,
        sum(quantity) as total_quantity,
        sum(unit_price_cents * quantity) as gross_item_cents,
        sum(discount_cents) as item_discount_cents
    from {{ ref('stg_order_items') }}
    group by order_id
),

final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.status,
        orders.order_date,
        orders.total_cents,
        orders.discount_code,
        orders.shipping_cents,
        orders.session_id,
        customers.full_name as customer_name,
        customers.email as customer_email,
        customers.loyalty_tier,
        coalesce(order_items.item_count, 0) as item_count,
        coalesce(order_items.total_quantity, 0) as total_quantity,
        coalesce(order_items.gross_item_cents, 0) as gross_item_cents
    from orders
    left join customers
        on orders.customer_id = customers.customer_id
    left join order_items
        on orders.order_id = order_items.order_id
)

select * from final
