{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (

    select * from {{ source('raw', 'marketing_spend') }}

),

renamed as (

    select
        spend_id,
        channel,
        date,
        spend_cents,
        impressions,
        clicks
    from source

)

select * from renamed
