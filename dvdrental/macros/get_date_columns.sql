-- macros/get_date_columns.sql
{% macro get_date_columns(table_name, date_column) %}
    DATE({{ date_column }}) AS {{ date_column }}_day,
    EXTRACT(YEAR FROM {{ date_column }}) AS {{ date_column }}_year,
    EXTRACT(MONTH FROM {{ date_column }}) AS {{ date_column }}_month,
    EXTRACT(DAY FROM {{ date_column }}) AS {{ date_column }}_day_of_month,
    EXTRACT(DOW FROM {{ date_column }}) AS {{ date_column }}_day_of_week,
    CASE 
        WHEN EXTRACT(DOW FROM {{ date_column }}) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS {{ date_column }}_weekend_flag
{% endmacro %}
