{{ config(
    materialized='table',
    tags=['marts']
) }}

with product_performance as (
    select * from {{ ref('int_product_performance') }}
),

final as (
    select
        product_id,
        product_name,
        category,
        subcategory,
        retail_price_cents,
        cost_price_cents,
        retail_price_cents - cost_price_cents as margin_cents,
        units_sold,
        revenue_cents,
        review_count,
        avg_rating,
        return_count,
        case
            when units_sold > 0 then return_count * 100.0 / units_sold
            else 0
        end as return_rate_pct
    from product_performance
)

select * from final
