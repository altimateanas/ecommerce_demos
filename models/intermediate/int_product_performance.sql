{{ config(
    materialized='table',
    tags=['intermediate']
) }}

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

reviews as (
    select * from {{ ref('stg_reviews') }}
),

returns as (
    select * from {{ ref('stg_returns') }}
),

item_agg as (
    select
        product_id,
        sum(quantity) as units_sold,
        sum(unit_price_cents * quantity) as revenue_cents,
        sum(discount_cents) as discount_cents
    from order_items
    group by product_id
),

review_agg as (
    select
        product_id,
        count(*) as review_count,
        avg(rating) as avg_rating
    from reviews
    group by product_id
),

return_agg as (
    select
        oi.product_id,
        count(*) as return_count
    from returns r
    join order_items oi on r.order_item_id = oi.item_id
    group by oi.product_id
),

final as (
    select
        products.product_id,
        products.product_name,
        products.category,
        products.subcategory,
        products.retail_price_cents,
        products.cost_price_cents,
        coalesce(item_agg.units_sold, 0) as units_sold,
        coalesce(item_agg.revenue_cents, 0) as revenue_cents,
        coalesce(review_agg.review_count, 0) as review_count,
        coalesce(review_agg.avg_rating, 0) as avg_rating,
        coalesce(return_agg.return_count, 0) as return_count
    from products
    left join item_agg on products.product_id = item_agg.product_id
    left join review_agg on products.product_id = review_agg.product_id
    left join return_agg on products.product_id = return_agg.product_id
)

select * from final
