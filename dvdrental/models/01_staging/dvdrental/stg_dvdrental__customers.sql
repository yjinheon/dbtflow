-- models/staging/dvdrental/stg_dvdrental__customers.sql
{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source AS (
    SELECT * FROM {{ source('dvdrental', 'customer') }}
),

address_source AS (
    SELECT * FROM {{ source('dvdrental', 'address') }}
),

city_source AS (
    SELECT * FROM {{ source('dvdrental', 'city') }}
),

country_source AS (
    SELECT * FROM {{ source('dvdrental', 'country') }}
),

cleaned AS (
    SELECT
        -- 기본 식별자
        c.customer_id,
        c.store_id,
        
        -- 고객 정보 정제
        TRIM(UPPER(c.first_name)) AS first_name,
        TRIM(UPPER(c.last_name)) AS last_name,
        LOWER(TRIM(c.email)) AS email,
        
        -- 주소 정보 조인
        a.address,
        a.district,
        city.city,
        country.country,
        a.postal_code,
        a.phone,
        
        -- 상태 정보
        CASE 
            WHEN c.active = 1 THEN TRUE 
            ELSE FALSE 
        END AS is_active,
        
        -- 날짜 정보
        c.create_date::DATE AS customer_create_date,
        c.last_update,
        
        -- 파생 컬럼들
        CONCAT(TRIM(c.first_name), ' ', TRIM(c.last_name)) AS full_name,
        
        -- 이메일 도메인 추출
        SPLIT_PART(c.email, '@', 2) AS email_domain,
        
        -- 지역 분류
        CASE 
            WHEN country.country IN ('United States', 'Canada') THEN 'North America'
            WHEN country.country IN ('United Kingdom', 'Germany', 'France', 'Italy', 'Spain') THEN 'Europe'
            WHEN country.country IN ('Japan', 'China', 'India') THEN 'Asia'
            ELSE 'Other'
        END AS region,
        
        -- 서로게이트 키 생성
        {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} AS customer_key,
        
        -- 해시키 (SCD Type 2용)
        {{ dbt_utils.generate_surrogate_key([
            'c.first_name', 
            'c.last_name', 
            'c.email', 
            'c.active',
            'a.address',
            'city.city',
            'country.country'
        ]) }} AS customer_hash_key
        
    FROM source c
    LEFT JOIN address_source a ON c.address_id = a.address_id
    LEFT JOIN city_source city ON a.city_id = city.city_id
    LEFT JOIN country_source country ON city.country_id = country.country_id
)

SELECT * FROM cleaned
