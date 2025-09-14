-- snapshots/scd_customers.sql
{% snapshot scd_customers_snapshot %}

    {{
        config(
          target_schema='snapshots',
          unique_key='customer_id',
          strategy='timestamp',
          updated_at='last_update',
          invalidate_hard_deletes=True
        )
    }}

    SELECT 
        customer_id,
        first_name,
        last_name,
        email,
        address,
        city,
        country,
        is_active,
        last_update
    FROM {{ ref('stg_dvdrental__customers') }}

{% endsnapshot %}
