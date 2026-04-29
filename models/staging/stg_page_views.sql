{{ config(
    materialized='view',
    tags=['staging', 'snowflake_dialect']
) }}with source as (

    select * from {{ source('raw', 'page_views') }}

),

renamed as (

    select
        page_view_id,
        session_id,
        customer_id,
        page_url,
        page_type,
        referrer,
        utm_source,
        utm_medium,
        utm_campaign,
        device_type,
        event_properties,
        created_at
    from source

)


select * from renamed