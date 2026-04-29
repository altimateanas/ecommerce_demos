{{ config(
    materialized='table',
    tags=['marts']
) }}with returns as (

    select * from {{ ref('stg_returns') }}

)
,

orders as (

    select * from {{ ref('stg_orders') }}

)
,

products as (

    select * from {{ ref('stg_products') }}

)
,

final as (

    select
        -- Primary key

        -- Dimensions




    from returns
    left join orders
        on returns.customer_id = orders.customer_id
    left join products
        on returns.customer_id = products.customer_id

)

select * from final
