{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (

    select * from {{ source('raw', 'orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        status,
        order_date,
        total_cents,
        discount_code,
        shipping_cents,
        session_id
    from source

)

select * from renamed
