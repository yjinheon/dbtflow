-- macros/data_quality_checks.sql
{% macro generate_data_quality_tests(table_name, columns) %}
    {% for column in columns %}
        {% if column.get('not_null', false) %}
      SELECT COUNT(*) as null_count
      FROM {{ table_name }}
      WHERE {{ column.name }} IS NULL
      {% if not loop.last %} UNION ALL {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}
