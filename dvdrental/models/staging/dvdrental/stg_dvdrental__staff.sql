-- models/staging/dvdrental/stg_dvdrental__staff.sql
{{ config(
    materialized='view', 
    schema='staging'
) }}

WITH staff_source AS (
    SELECT * FROM {{ source('dvdrental', 'staff') }}
),

address_source AS (
    SELECT * FROM {{ source('dvdrental', 'address') }}
),

cleaned AS (
    SELECT
        s.staff_id,
        s.store_id,
        TRIM(s.first_name) AS first_name,
        TRIM(s.last_name) AS last_name,
        LOWER(TRIM(s.email)) AS email,
        s.active AS is_active,
        s.username,
        s.last_update,
        -- 주소 정보
        a.address,
        a.district,
        a.postal_code,
        a.phone,
        -- 파생 컬럼
        CONCAT(s.first_name, ' ', s.last_name) AS full_name,
        
        -- 키 생성
        {{ dbt_utils.generate_surrogate_key(['s.staff_id']) }} AS staff_key
        
    FROM staff_source s
    LEFT JOIN address_source a ON s.address_id = a.address_id
)

SELECT * FROM cleaned
