{% test reasonable_lap_time(model, column_name, min_seconds=30, max_seconds=300) %}

-- Custom generic test to ensure lap times are within reasonable F1 race bounds
-- Usage: - reasonable_lap_time:
--           min_seconds: 30
--           max_seconds: 300

with validation as (
    select
        {{ column_name }} as lap_time,
        count(*) as violation_count
    from {{ model }}
    where {{ column_name }} is not null
      and ({{ column_name }} < {{ min_seconds }} or {{ column_name }} > {{ max_seconds }})
    group by {{ column_name }}
)

select *
from validation
where violation_count > 0

{% endtest %}
