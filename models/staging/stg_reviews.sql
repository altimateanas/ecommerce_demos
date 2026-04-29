{{ config(
    materialized='view',
    tags=['staging']
) }}with source as (

    select * from {{ source('raw', 'reviews') }}

),

renamed as (

    select
        review_id,
        product_id,
        customer_id,
        rating,
        title,
        body,
        is_verified_purchase,
        helpful_votes,
        created_at
    from source

)


select * from renamed