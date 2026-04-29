{{ config(
    materialized='table',
    tags=['intermediate']
) }}with orders as (

    select * from {{ ref('stg_orders') }}

)
,

customers as (

    select * from {{ ref('stg_customers') }}

)
,

returns as (

    select * from {{ ref('stg_returns') }}

)
,

joined as (

    select
        orders.*,
    from orders
    left join customers
        on orders.customer_id = customers.customer_id
    left join returns
        on orders.customer_id = returns.customer_id

)

select * from joined
