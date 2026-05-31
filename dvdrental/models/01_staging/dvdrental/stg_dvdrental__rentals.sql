-- models/staging/dvdrental/stg_dvdrental__rentals.sql
{{ config(
    materialized='view',
    schema='staging'
) }}

{% set rental_date_columns = get_date_columns('rental', 'rental_date') %}

WITH source AS (
    SELECT * FROM {{ source('dvdrental', 'rental') }}
),

inventory AS (
    SELECT * FROM {{ source('dvdrental', 'inventory') }}
),

cleaned AS (
    SELECT
        -- 기본 식별자
        r.rental_id,
        r.customer_id,
        r.staff_id,
        i.film_id,
        i.store_id,
        r.inventory_id,
        
        -- 날짜 정보
        r.rental_date,
        r.return_date,
        r.last_update,
        
        -- 매크로를 사용한 날짜 파생 컬럼들
        {{ get_date_columns('r', 'rental_date') }},
        
        -- 반납 관련 계산
        CASE 
            WHEN r.return_date IS NOT NULL 
            THEN r.return_date::DATE
            ELSE NULL 
        END AS return_date_day,
        
        CASE 
            WHEN r.return_date IS NOT NULL 
            THEN EXTRACT(DAY FROM r.return_date - r.rental_date)
            ELSE NULL 
        END AS actual_rental_duration_days,
        
        CASE 
            WHEN r.return_date IS NULL THEN TRUE 
            ELSE FALSE 
        END AS is_currently_rented,
        
        -- 대여 기간 분류
        CASE 
            WHEN r.return_date IS NULL THEN 'Not Returned'
            WHEN EXTRACT(DAY FROM r.return_date - r.rental_date) <= 3 THEN 'Short Term'
            WHEN EXTRACT(DAY FROM r.return_date - r.rental_date) <= 7 THEN 'Medium Term'
            ELSE 'Long Term'
        END AS rental_duration_category,
        
        -- 시간대 분석
        CASE 
            WHEN EXTRACT(HOUR FROM r.rental_date) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM r.rental_date) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM r.rental_date) BETWEEN 18 AND 22 THEN 'Evening'
            ELSE 'Night'
        END AS rental_time_of_day,
        
        -- 성수기/비수기 판단 (여름: 성수기)
        CASE 
            WHEN EXTRACT(MONTH FROM r.rental_date) IN (6, 7, 8) THEN 'Peak Season'
            WHEN EXTRACT(MONTH FROM r.rental_date) IN (12, 1, 2) THEN 'Holiday Season'
            ELSE 'Regular Season'
        END AS seasonal_category,
        
        -- 키 생성
        {{ dbt_utils.generate_surrogate_key(['r.rental_id']) }} AS rental_key,
        {{ dbt_utils.generate_surrogate_key(['r.customer_id', 'i.film_id', 'r.rental_date']) }} AS rental_business_key
        
    FROM source r
    JOIN inventory i ON r.inventory_id = i.inventory_id
)

SELECT * FROM cleaned
