{% macro log_model_start_end(model, event_name) %}
    {{ return("select 1 as dbt_hook_noop") }}
{% endmacro %}
