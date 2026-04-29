

{% macro date_spine(start_date, end_date, interval='day') %}

{% if target.type == 'duckdb' %}
    {# DuckDB: use generate_series #}
    select
        cast(generated_date as date) as date_{{ interval }}
    from (
        select
            unnest(
                generate_series(
                    cast('{{ start_date }}' as date),
                    cast('{{ end_date }}' as date),
                    interval '1 {{ interval }}'
                )
            ) as generated_date
    )

{% elif target.type == 'snowflake' %}
    {# Snowflake: use GENERATOR + DATEADD #}
    select
        dateadd(
            '{{ interval }}',
            seq4(),
            to_date('{{ start_date }}')
        ) as date_{{ interval }}
    from table(generator(rowcount => datediff('{{ interval }}', '{{ start_date }}'::date, '{{ end_date }}'::date) + 1))

{% elif target.type == 'postgres' %}
    {# PostgreSQL: use generate_series #}
    select
        generated_date::date as date_{{ interval }}
    from generate_series(
        '{{ start_date }}'::date,
        '{{ end_date }}'::date,
        '1 {{ interval }}'::interval
    ) as generated_date

{% else %}
    {# Generic fallback: recursive CTE #}
    with recursive date_series as (
        select cast('{{ start_date }}' as date) as date_{{ interval }}
        union all
        select date_{{ interval }} + interval '1 {{ interval }}'
        from date_series
        where date_{{ interval }} < cast('{{ end_date }}' as date)
    )
    select date_{{ interval }} from date_series

{% endif %}

{% endmacro %}
