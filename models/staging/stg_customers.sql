{{ config(
    materialized='view',
    tags=['staging']
) }}with source as (

    select * from {{ source('raw', 'customers') }}

),

renamed as (

    select
        customer_id,
        email,
        full_name,
        phone,
        shipping_address,
        billing_address,
        city,
        state,
        country,
        segment,
        acquisition_channel,
        created_at,
        updated_at
    from source

)


select * from renamed