{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (

    select * from {{ source('raw', 'page_views') }}

),

renamed as (

    select
        view_id,
        session_id,
        customer_id,
        url,
        utm_params,
        device,
        referrer,
        viewed_at
    from source

)

select * from renamed
