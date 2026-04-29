{{ config(
    materialized='table',
    tags=['marts']
) }}

with returns as (
    select * from {{ ref('stg_returns') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        returns.return_id,
        returns.order_id,
        returns.order_item_id,
        returns.reason_code,
        returns.refund_cents,
        returns.restocking_fee_cents,
        returns.returned_at,
        orders.customer_id,
        orders.order_date,
        products.product_id,
        products.product_name,
        products.category
    from returns
    left join orders
        on returns.order_id = orders.order_id
    left join order_items
        on returns.order_item_id = order_items.item_id
    left join products
        on order_items.product_id = products.product_id
)

select * from final
