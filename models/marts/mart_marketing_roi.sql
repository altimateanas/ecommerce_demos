{{ config(
    materialized='table',
    tags=['marts']
) }}

with marketing as (
    select * from {{ ref('int_marketing_attribution') }}
),

final as (
    select
        spend_id,
        channel,
        spend_date,
        spend_cents,
        impressions,
        clicks,
        click_through_rate,
        cost_per_click_cents
    from marketing
)

select * from final
