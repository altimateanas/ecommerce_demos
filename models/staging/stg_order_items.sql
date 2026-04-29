{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (

    select * from {{ source('raw', 'order_items') }}

),

renamed as (

    select
        item_id,
        order_id,
        product_id,
        quantity,
        unit_price_cents,
        discount_cents,
        created_at
    from source

)

select * from renamed
