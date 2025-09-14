-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ var('schema_prefix', '') }}{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
