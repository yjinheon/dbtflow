-- models/marts/core/dim_staff.sql
{{ config(
    materialized='table',
    schema='marts'
) }}

WITH staff_enriched AS (
    SELECT
        -- 서로게이트 키
        {{ dbt_utils.generate_surrogate_key(['staff_id']) }} AS staff_dim_key,
        
        -- 비즈니스 키
        staff_id AS staff_business_key,
        store_id,
        
        -- 직원 정보
        first_name,
        last_name,
        full_name,
        email,
        is_active,
        username,
        
        -- 주소 정보
        address,
        district,
        postal_code,
        phone,
        
        -- 파생 속성
        CASE 
            WHEN is_active THEN 'Active'
            ELSE 'Inactive'
        END AS employment_status,
        
        -- 이메일 도메인
        SPLIT_PART(email, '@', 2) AS email_domain,
        
        -- 메타데이터
        last_update AS source_last_update,
        CURRENT_TIMESTAMP AS loaded_at
        
    FROM {{ ref('stg_dvdrental__staff') }}
)

SELECT * FROM staff_enriched
