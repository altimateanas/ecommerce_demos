{{ config(
    materialized='table',
    tags=['intermediate']
) }}with orders as (

    select * from {{ ref('stg_orders') }}

)
,

customers as (

    select * from {{ ref('stg_customers') }}

)
,

order_items as (

    select * from {{ ref('stg_order_items') }}

)
,

products as (

    select * from {{ ref('stg_products') }}

)
,

joined as (

    select
        orders.*,
    from orders
    left join customers
        on orders.customer_id = customers.customer_id
    left join order_items
        on orders.customer_id = order_items.customer_id
    left join products
        on orders.customer_id = products.customer_id

)

select * from joined
