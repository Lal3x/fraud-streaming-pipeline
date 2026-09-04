{% test non_negative(model, column_name) %}
select * from {{ model }} where {{ column_name }} < 0
{% endtest %}

{% test accepted_range(model, column_name, min_value, max_value) %}
select * from {{ model }}
where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}

{% test column_absent(model, column_name) %}
select 1
from information_schema.columns
where table_schema = '{{ model.schema }}'
  and table_name = '{{ model.identifier }}'
  and column_name = '{{ column_name }}'
{% endtest %}
