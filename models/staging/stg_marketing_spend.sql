{{ config(
    materialized='view',
    tags=['staging']
) }}with source as (

    select * from {{ source('raw', 'marketing_spend') }}

),

renamed as (

    select
        spend_id,
        campaign_name,
        channel,
        spend_cents,
        impressions,
        clicks,
        conversions,
        date
    from source

)


select * from renamed