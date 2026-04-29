

{% test assert_no_future_dates(model, column_name, tolerance_hours=1) %}

-- This test returns rows where {{ column_name }} is in the future.
-- A tolerance of {{ tolerance_hours }} hour(s) is allowed for clock skew.
-- Any row returned means the test FAILS.

select
    *
from {{ model }}
where
    {{ column_name }} is not null
    and {{ column_name }} > {{ dbt.dateadd('hour', tolerance_hours, dbt.current_timestamp()) }}

{% endtest %}
