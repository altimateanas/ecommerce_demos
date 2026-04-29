{{ config(
    materialized='table',
    schema='marts_pii',
    tags=['marts', 'pii', 'restricted'],
    meta={
        'contains_pii': true,
        'owner': 'data-governance',
        'compliance': 'CCPA / GDPR'
    }
) }}with customers as (

    select * from {{ ref('stg_customers') }}

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

returns as (

    select * from {{ ref('stg_returns') }}

)
,

reviews as (

    select * from {{ ref('stg_reviews') }}

)
,

final as (

    select
        -- Primary key

        -- Dimensions


        -- PII columns (restricted access)
        -- CCPA / GDPR: These fields require role-based access control
        email,
        full_name,
        phone,
        shipping_address,
        billing_address


    from customers
    left join orders
        on customers.customer_id = orders.customer_id
    left join page_views
        on customers.customer_id = page_views.customer_id
    left join returns
        on customers.customer_id = returns.customer_id
    left join reviews
        on customers.customer_id = reviews.customer_id

)

select * from final
