{% test valid_percentage(model, column_name) %}

-- Custom generic test to ensure percentage values are between 0 and 100
-- Usage: - valid_percentage

select
    {{ column_name }} as percentage_value,
    count(*) as violation_count
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < 0 or {{ column_name }} > 100)
group by {{ column_name }}

{% endtest %}
