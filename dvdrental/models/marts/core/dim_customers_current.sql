-- models/marts/core/dim_customers_current.sql
{{ config(
    materialized='view',
    schema='marts'
) }}

-- 현재 유효한 고객 레코드만 조회하는 뷰
SELECT *
FROM {{ ref('dim_customers') }}
WHERE is_current_record = TRUE
