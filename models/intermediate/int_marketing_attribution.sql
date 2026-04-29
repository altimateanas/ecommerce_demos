{{ config(
    materialized='table',
    tags=['intermediate']
) }}with marketing_spend as (

    select * from {{ ref('stg_marketing_spend') }}

)
,

orders as (

    select * from {{ ref('stg_orders') }}

)
,

page_views as (

    select * from {{ ref('stg_page_views') }}

)
,

joined as (

    select
        marketing_spend.*,
    from marketing_spend
    left join orders
        on marketing_spend.customer_id = orders.customer_id
    left join page_views
        on marketing_spend.customer_id = page_views.customer_id

)

select * from joined
