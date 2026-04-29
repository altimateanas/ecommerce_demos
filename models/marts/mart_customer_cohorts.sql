{{ config(
    materialized='table',
    tags=['marts']
) }}with customer_lifetime as (

    select * from {{ ref('int_customer_lifetime') }}

)
,

customers as (

    select * from {{ ref('stg_customers') }}

)
,

final as (

    select
        -- Primary key

        -- Dimensions




    from customer_lifetime
    left join customers
        on customer_lifetime.customer_id = customers.customer_id

)

select * from final
