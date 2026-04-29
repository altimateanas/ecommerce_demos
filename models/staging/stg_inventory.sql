{{ config(
    materialized='view',
    tags=['staging']
) }}with source as (

    select * from {{ source('raw', 'inventory') }}

),

renamed as (

    select
        inventory_id,
        product_id,
        warehouse_id,
        quantity_on_hand,
        quantity_reserved,
        quantity_available,
        reorder_point,
        last_restock_at,
        updated_at
    from source

)


select * from renamed