-- macros/pivot_rental_metrics.sql
{% macro pivot_rental_metrics(column_name, values, metric='count') %}
    {% for value in values %}
        {{ metric }}(
      CASE WHEN {{ column_name }} = '{{ value }}' THEN 1 END
    ) AS {{ metric }}_{{ value | lower | replace(' ', '_') | replace('-', '_') }}
        {%- if not loop.last -%},{%- endif %}
    {% endfor %}
{% endmacro %}
