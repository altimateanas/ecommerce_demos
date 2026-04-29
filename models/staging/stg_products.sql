{{ config(
    materialized='view',
    tags=['staging']
) }}with source as (

    select * from {{ source('raw', 'products') }}

),

renamed as (

    select
        product_id,
        product_name,
        sku,
        category,
        subcategory,
        brand,
        price_cents,
        cost_cents,
        weight_grams,
        is_active,
        created_at
    from source

)


select * from renamed