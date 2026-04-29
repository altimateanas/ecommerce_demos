

{% test assert_positive_amounts(model, column_name) %}

-- This test returns rows where {{ column_name }} is not positive.
-- Any row returned means the test FAILS.

select
    *
from {{ model }}
where
    {{ column_name }} is not null
    and {{ column_name }} <= 0

{% endtest %}
