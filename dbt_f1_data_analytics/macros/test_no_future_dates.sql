{% test no_future_dates(model, column_name) %}

-- Custom generic test to ensure dates are not in the future
-- Usage: - no_future_dates

select
    {{ column_name }} as date_value,
    count(*) as violation_count
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} > current_timestamp()
group by {{ column_name }}

{% endtest %}
