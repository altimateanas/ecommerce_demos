{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (

    select * from {{ source('raw', 'inventory') }}

),

renamed as (

    select
        inventory_id,
        warehouse_id,
        product_id,
        quantity_on_hand,
        reorder_point,
        last_restocked_at
    from source

)

select * from renamed
