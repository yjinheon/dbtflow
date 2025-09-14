-- models/staging/dvdrental/stg_dvdrental__films.sql
{{ config(
    materialized='view',
    schema='staging'
) }}

WITH films AS (
    SELECT * FROM {{ source('dvdrental', 'film') }}
),

film_categories AS (
    SELECT * FROM {{ source('dvdrental', 'film_category') }}
),

categories AS (
    SELECT * FROM {{ source('dvdrental', 'category') }}
),

language AS (
    SELECT * FROM {{ source('dvdrental', 'language') }}
),

-- 영화별 카테고리 집계 (영화는 여러 카테고리 가능)
film_category_agg AS (
    SELECT 
        fc.film_id,
        STRING_AGG(c.name, ', ' ORDER BY c.name) AS categories,
        COUNT(c.category_id) AS category_count,
        ARRAY_AGG(c.name ORDER BY c.name) AS category_array
    FROM film_categories fc
    JOIN categories c ON fc.category_id = c.category_id
    GROUP BY fc.film_id
),

cleaned AS (
    SELECT
        -- 기본 식별자
        f.film_id,
        f.language_id,
        
        -- 영화 정보 정제
        TRIM(f.title) AS title,
        TRIM(f.description) AS description,
        f.release_year,
        l.name AS language_name,
        
        -- 카테고리 정보
        COALESCE(fca.categories, 'Uncategorized') AS categories,
        COALESCE(fca.category_count, 0) AS category_count,
        COALESCE(fca.category_array, ARRAY['Uncategorized']) AS category_array,
        
        -- 대여 정보
        f.rental_duration,
        f.rental_rate,
        f.replacement_cost,
        
        -- 영화 메타데이터
        f.length AS duration_minutes,
        f.rating,
        f.special_features,
        f.last_update,
        
        -- 파생 컬럼들
        CASE 
            WHEN f.length IS NULL THEN 'Unknown'
            WHEN f.length < 90 THEN 'Short'
            WHEN f.length BETWEEN 90 AND 120 THEN 'Medium'
            WHEN f.length BETWEEN 121 AND 150 THEN 'Long'
            ELSE 'Very Long'
        END AS duration_category,
        
        CASE 
            WHEN f.rental_rate < 1.00 THEN 'Budget'
            WHEN f.rental_rate BETWEEN 1.00 AND 2.99 THEN 'Standard'
            WHEN f.rental_rate BETWEEN 3.00 AND 4.99 THEN 'Premium'
            ELSE 'Luxury'
        END AS price_tier,
        
        CASE 
            WHEN f.rating IN ('G', 'PG') THEN 'Family'
            WHEN f.rating IN ('PG-13') THEN 'Teen'
            WHEN f.rating IN ('R', 'NC-17') THEN 'Adult'
            ELSE 'Unrated'
        END AS audience_category,
        
        -- 수익성 지표
        ROUND(f.rental_rate / NULLIF(f.rental_duration, 0), 2) AS daily_revenue_potential,
        
        -- 키 생성
        {{ dbt_utils.generate_surrogate_key(['f.film_id']) }} AS film_key
        
    FROM films f
    LEFT JOIN language l ON f.language_id = l.language_id
    LEFT JOIN film_category_agg fca ON f.film_id = fca.film_id
)

SELECT * FROM cleaned
