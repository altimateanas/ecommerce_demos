{{ config(
    materialized='table',
    tags=['intermediate']
) }}

with marketing_spend as (
    select * from {{ ref('stg_marketing_spend') }}
),

final as (
    select
        spend_id,
        channel,
        date as spend_date,
        spend_cents,
        impressions,
        clicks,
        case when impressions > 0 then clicks * 1.0 / impressions else 0 end as click_through_rate,
        case when clicks > 0 then spend_cents * 1.0 / clicks else 0 end as cost_per_click_cents
    from marketing_spend
)

select * from final
