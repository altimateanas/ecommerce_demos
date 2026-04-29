{{ config(
    materialized='table',
    tags=['intermediate']
) }}

with page_views as (
    select * from {{ ref('stg_page_views') }}
),

final as (
    select
        session_id,
        customer_id,
        min(viewed_at) as session_start,
        max(viewed_at) as session_end,
        count(*) as page_views,
        count(distinct url) as unique_pages
    from page_views
    group by session_id, customer_id
)

select * from final
