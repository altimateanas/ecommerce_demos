{{ config(
    materialized='table',
    tags=['intermediate']
) }}

with inventory as (
    select * from {{ ref('stg_inventory') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

warehouses as (
    select * from {{ ref('stg_warehouses') }}
),

final as (
    select
        inventory.inventory_id,
        inventory.product_id,
        inventory.warehouse_id,
        products.product_name,
        products.category,
        warehouses.warehouse_name,
        warehouses.city as warehouse_city,
        inventory.quantity_on_hand,
        inventory.reorder_point,
        inventory.last_restocked_at,
        case
            when inventory.quantity_on_hand <= inventory.reorder_point then 'reorder_needed'
            when inventory.quantity_on_hand <= inventory.reorder_point * 2 then 'low_stock'
            else 'healthy'
        end as stock_status
    from inventory
    left join products
        on inventory.product_id = products.product_id
    left join warehouses
        on inventory.warehouse_id = warehouses.warehouse_id
)

select * from final
