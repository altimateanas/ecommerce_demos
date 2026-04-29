{{ config(
    materialized='view',
    tags=['staging']
) }}with source as (

    select * from {{ source('raw', 'returns') }}

),

renamed as (

    select
        return_id,
        order_id,
        customer_id,
        reason,
        refund_amount_cents,
        status,
        requested_at,
        processed_at
    from source

)


select * from renamed