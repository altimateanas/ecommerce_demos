{{ config(
    materialized='view',
    tags=['staging']
) }}with source as (

    select * from {{ source('raw', 'orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        order_date,
        status,
        subtotal_cents,
        discount_cents,
        shipping_cents,
        tax_cents,
        total_cents,
        coupon_code,
        payment_method,
        shipping_method,
        warehouse_id
    from source

)


select * from renamed