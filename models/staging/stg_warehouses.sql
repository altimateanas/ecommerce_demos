{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (

    select * from {{ source('raw', 'warehouses') }}

),

renamed as (

    select
        warehouse_id,
        name as warehouse_name,
        city,
        state,
        capacity,
        type
    from source

)

select * from renamed
