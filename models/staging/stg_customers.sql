{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (

    select * from {{ source('raw', 'customers') }}

),

renamed as (

    select
        customer_id,
        full_name,
        email,
        phone,
        shipping_address,
        billing_address,
        loyalty_tier,
        created_at
    from source

)

select * from renamed
