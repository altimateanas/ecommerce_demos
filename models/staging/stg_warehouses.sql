{{ config(
    materialized='view',
    tags=['staging']
) }}with source as (

    select * from {{ source('raw', 'warehouses') }}

),

renamed as (

    select
        warehouse_id,
        warehouse_name,
        city,
        state,
        country,
        capacity_sqft,
        is_active
    from source

)


select * from renamed