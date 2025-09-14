-- macros/get_rental_status_columns.sql
{% macro get_rental_status_columns() %}
    {% if var('include_rental_status', true) %}
    , CASE 
        WHEN return_date IS NULL THEN 'Active'
        WHEN return_date > rental_date + INTERVAL '7 days' THEN 'Overdue'
        ELSE 'Returned'
      END AS rental_status
  {% endif %}
  
    {% if var('include_late_fees', false) %}
    , CASE 
        WHEN return_date > rental_date + INTERVAL '7 days' 
        THEN (EXTRACT(DAY FROM return_date - rental_date) - 7) * 1.5
        ELSE 0
      END AS late_fee_amount
  {% endif %}
{% endmacro %}
