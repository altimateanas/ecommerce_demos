{{ config(
    materialized='table',
    tags=['intermediate']
) }}with inventory as (

    select * from {{ ref('stg_inventory') }}

)
,

products as (

    select * from {{ ref('stg_products') }}

)
,

warehouses as (

    select * from {{ ref('stg_warehouses') }}

)
,

joined as (

    select
        inventory.*,
    from inventory
    left join products
        on inventory.customer_id = products.customer_id
    left join warehouses
        on inventory.customer_id = warehouses.customer_id

)

select * from joined
