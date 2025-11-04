{% test positive_values(model, column_name) %}

-- Custom generic test to ensure column contains only positive values (> 0)
-- Usage: - positive_values

select
    {{ column_name }} as value,
    count(*) as violation_count
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} <= 0
group by {{ column_name }}

{% endtest %}
