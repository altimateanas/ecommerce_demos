{{ config(
    materialized='table',
    tags=['intermediate', 'snowflake_dialect']
) }}with page_views as (

    select * from {{ ref('stg_page_views') }}

)
,

joined as (

    select
        page_views.*
    from page_views

)

select * from joined
