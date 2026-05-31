-- models/03_marts/core/dim_stores.sql
{{ config(
    materialized='table',
    schema='marts'
) }}

WITH stores AS (
    SELECT * FROM {{ ref('stg_dvdrental__stores') }}
),

dim_stores AS (
    SELECT
        -- 서로게이트 키
        {{ dbt_utils.generate_surrogate_key(['store_id']) }} AS store_dim_key,
        
        -- 비즈니스 키
        store_id AS store_business_key,
        
        -- 매장 정보
        manager_staff_id,
        address,
        district,
        city,
        country,
        postal_code,
        
        -- 지역 분류
        CASE 
            WHEN country IN ('United States', 'Canada') THEN 'North America'
            WHEN country IN ('United Kingdom', 'Germany', 'France', 'Italy', 'Spain') THEN 'Europe'
            WHEN country IN ('Japan', 'China', 'India') THEN 'Asia'
            ELSE 'Other'
        END AS region,
        
        -- 매장 식별자
        CONCAT('Store ', store_id, ' - ', city) AS store_name,
        
        -- 메타데이터
        last_update AS source_last_update,
        CURRENT_TIMESTAMP AS loaded_at
        
    FROM stores
)

SELECT * FROM dim_stores
