{{ config(
    materialized='table',
    tags=['intermediate']
) }}with order_items as (

    select * from {{ ref('stg_order_items') }}

)
,

products as (

    select * from {{ ref('stg_products') }}

)
,

reviews as (

    select * from {{ ref('stg_reviews') }}

)
,

returns as (

    select * from {{ ref('stg_returns') }}

)
,

joined as (

    select
        order_items.*,
    from order_items
    left join products
        on order_items.customer_id = products.customer_id
    left join reviews
        on order_items.customer_id = reviews.customer_id
    left join returns
        on order_items.customer_id = returns.customer_id

)

select * from joined
