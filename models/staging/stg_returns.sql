{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (

    select * from {{ source('raw', 'returns') }}

),

renamed as (

    select
        return_id,
        order_id,
        order_item_id,
        reason_code,
        refund_cents,
        restocking_fee_cents,
        returned_at
    from source

)

select * from renamed
