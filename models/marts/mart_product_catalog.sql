{{ config(
    materialized='table',
    tags=['marts']
) }}with product_performance as (

    select * from {{ ref('int_product_performance') }}

)
,

inventory as (

    select * from {{ ref('stg_inventory') }}

)
,

final as (

    select
        -- Primary key

        -- Dimensions




    from product_performance
    left join inventory
        on product_performance.customer_id = inventory.customer_id

)

select * from final
