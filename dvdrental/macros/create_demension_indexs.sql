-- macros/create_dimension_indexes.sql
{% macro create_dimension_indexes() %}

  {% set indexes = [
    {
      'table': 'dim_customers',
      'columns': ['customer_business_key', 'is_current_record'],
      'type': 'btree'
    },
    {
      'table': 'dim_customers', 
      'columns': ['effective_start_date', 'effective_end_date'],
      'type': 'btree'
    },
    {
      'table': 'dim_films',
      'columns': ['film_business_key'],
      'type': 'btree'
    },
    {
      'table': 'dim_films',
      'columns': ['genre_group', 'price_tier'],
      'type': 'btree'
    }
  ] %}

    {% for index in indexes %}
    CREATE INDEX IF NOT EXISTS idx_{{ index.table }}_{{ index.columns | join('_') }}
    ON {{ target.schema }}.{{ index.table }} 
    USING {{ index.type }} ({{ index.columns | join(', ') }});
  {% endfor %}

{% endmacro %}
