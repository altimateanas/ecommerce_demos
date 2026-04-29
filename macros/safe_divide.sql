

{% macro safe_divide(numerator, denominator, default_value='null') %}
    case
        when {{ denominator }} is null or {{ denominator }} = 0
            then {{ default_value }}
        else cast({{ numerator }} as double precision) / cast({{ denominator }} as double precision)
    end
{% endmacro %}
