{{ config(
    materialized='table',
    tags=['marts']
) }}with marketing_attribution as (

    select * from {{ ref('int_marketing_attribution') }}

)
,

marketing_spend as (

    select * from {{ ref('stg_marketing_spend') }}

)
,

final as (

    select
        -- Primary key

        -- Dimensions




    from marketing_attribution
    left join marketing_spend
        on marketing_attribution.customer_id = marketing_spend.customer_id

)

select * from final
