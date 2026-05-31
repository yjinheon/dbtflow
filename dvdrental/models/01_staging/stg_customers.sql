-- models/staging/stg_customers.sql
{{ config(materialized='view') }}

-- view, table, incremental, ephemeral

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    active,
    create_date,
    last_update,
    -- 추가 컬럼들
    CONCAT(first_name, ' ', last_name) AS full_name,
    CASE
        WHEN active = 1 THEN 'Active'
        ELSE 'Inactive'
    END AS customer_status
FROM {{ source('dvdrental', 'customer') }}
