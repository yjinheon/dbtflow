-- models/staging/dvdrental/stg_dvdrental__payments.sql
{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source AS (
    SELECT * FROM {{ source('dvdrental', 'payment') }}
),

cleaned AS (
    SELECT
        -- 기본 식별자
        payment_id,
        customer_id,
        staff_id,
        rental_id,
        
        -- 결제 정보
        amount,
        payment_date,
        
        -- 날짜 파생 컬럼들
        {{ get_date_columns('', 'payment_date') }},
        
        -- 금액 분류
        CASE 
            WHEN amount < 2.00 THEN 'Low Value'
            WHEN amount BETWEEN 2.00 AND 4.99 THEN 'Medium Value'
            WHEN amount BETWEEN 5.00 AND 7.99 THEN 'High Value'
            ELSE 'Premium Value'
        END AS payment_tier,
        
        -- 결제 시간대 분석
        CASE 
            WHEN EXTRACT(HOUR FROM payment_date) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM payment_date) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM payment_date) BETWEEN 18 AND 22 THEN 'Evening'
            ELSE 'Night'
        END AS payment_time_of_day,
        
        -- 주말/평일 구분
        CASE 
            WHEN EXTRACT(DOW FROM payment_date) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END AS payment_day_type,
        
        -- 키 생성
        {{ dbt_utils.generate_surrogate_key(['payment_id']) }} AS payment_key
        
    FROM source
    WHERE amount > 0  -- 음수 결제 제외
)

SELECT * FROM cleaned
