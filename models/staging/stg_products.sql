{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (

    select * from {{ source('raw', 'products') }}

),

renamed as (

    select
        product_id,
        product_name,
        category,
        subcategory,
        cost_price_cents,
        retail_price_cents,
        weight_kg,
        sku,
        created_at
    from source

)

select * from renamed
