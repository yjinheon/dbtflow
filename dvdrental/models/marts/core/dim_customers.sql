-- models/marts/core/dim_customers.sql
{{ config(
    materialized='table',
    schema='marts'
) }}

WITH snapshot_data AS (
    SELECT 
        customer_id,
        first_name,
        last_name,
        email,
        address,
        city,
        country,
        is_active,
        last_update,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('scd_customers_snapshot') }}
),

dim_customers AS (
    SELECT
        -- 서로게이트 키 (차원 테이블의 PK)
        {{ dbt_utils.generate_surrogate_key(['dbt_scd_id']) }} AS customer_dim_key,
        
        -- 비즈니스 키 (원본 PK)
        customer_id AS customer_business_key,
        
        -- SCD Type 2 메타데이터
        dbt_valid_from AS effective_start_date,
        COALESCE(dbt_valid_to, '9999-12-31'::DATE) AS effective_end_date,
        CASE 
            WHEN dbt_valid_to IS NULL THEN TRUE 
            ELSE FALSE 
        END AS is_current_record,
        
        -- 고객 속성들
        TRIM(UPPER(first_name)) AS first_name,
        TRIM(UPPER(last_name)) AS last_name,
        CONCAT(TRIM(first_name), ' ', TRIM(last_name)) AS full_name,
        LOWER(TRIM(email)) AS email,
        address,
        city,
        country,
        is_active,
        
        -- 고객 분류
        CASE 
            WHEN country IN ('United States', 'Canada') THEN 'North America'
            WHEN country IN ('United Kingdom', 'Germany', 'France', 'Italy', 'Spain') THEN 'Europe'
            WHEN country IN ('Japan', 'China', 'India') THEN 'Asia'
            ELSE 'Other'
        END AS region,
        
        CASE 
            WHEN is_active THEN 'Active'
            ELSE 'Inactive'
        END AS customer_status,
        
        -- 이메일 도메인 분석
        SPLIT_PART(email, '@', 2) AS email_domain,
        CASE 
            WHEN SPLIT_PART(email, '@', 2) LIKE '%.com' THEN 'Commercial'
            WHEN SPLIT_PART(email, '@', 2) LIKE '%.org' THEN 'Organization'
            WHEN SPLIT_PART(email, '@', 2) LIKE '%.edu' THEN 'Educational'
            ELSE 'Other'
        END AS email_domain_type,
        
        -- 메타데이터
        last_update AS source_last_update,
        dbt_updated_at AS dimension_updated_at,
        CURRENT_TIMESTAMP AS loaded_at
        
    FROM snapshot_data
)

SELECT * FROM dim_customers
