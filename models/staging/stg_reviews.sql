{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (

    select * from {{ source('raw', 'reviews') }}

),

renamed as (

    select
        review_id,
        product_id,
        customer_id,
        rating,
        review_text,
        reviewer_name,
        created_at
    from source

)

select * from renamed
