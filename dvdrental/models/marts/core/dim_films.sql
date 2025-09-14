-- models/marts/core/dim_films.sql
{{ config(
    materialized='table',
    schema='marts'
) }}

WITH films_enriched AS (
    SELECT
        -- 서로게이트 키
        {{ dbt_utils.generate_surrogate_key(['film_id']) }} AS film_dim_key,
        
        -- 비즈니스 키
        film_id AS film_business_key,
        
        -- 영화 기본 정보
        title,
        description,
        release_year,
        language_name,
        
        -- 카테고리 정보
        categories,
        category_count,
        
        -- 물리적 속성
        duration_minutes,
        duration_category,
        rating,
        audience_category,
        special_features,
        
        -- 비즈니스 속성
        rental_duration,
        rental_rate,
        replacement_cost,
        price_tier,
        daily_revenue_potential,
        
        -- 영화 분류 (새로운 비즈니스 로직)
        CASE 
            WHEN duration_minutes < 90 AND rental_rate < 2.99 THEN 'Budget Short'
            WHEN duration_minutes < 90 AND rental_rate >= 2.99 THEN 'Premium Short'
            WHEN duration_minutes BETWEEN 90 AND 120 AND rental_rate < 2.99 THEN 'Budget Feature'
            WHEN duration_minutes BETWEEN 90 AND 120 AND rental_rate >= 2.99 THEN 'Premium Feature'
            WHEN duration_minutes > 120 AND rental_rate < 2.99 THEN 'Budget Epic'
            WHEN duration_minutes > 120 AND rental_rate >= 2.99 THEN 'Premium Epic'
            ELSE 'Other'
        END AS film_segment,
        
        -- 수익성 분석
        CASE 
            WHEN daily_revenue_potential >= 1.0 THEN 'High Revenue'
            WHEN daily_revenue_potential >= 0.5 THEN 'Medium Revenue'
            ELSE 'Low Revenue'
        END AS revenue_potential_category,
        
        -- 장르 기반 분류
        CASE 
            WHEN categories ILIKE '%Action%' OR categories ILIKE '%Adventure%' THEN 'Action/Adventure'
            WHEN categories ILIKE '%Comedy%' THEN 'Comedy'
            WHEN categories ILIKE '%Drama%' THEN 'Drama'
            WHEN categories ILIKE '%Horror%' OR categories ILIKE '%Sci-Fi%' THEN 'Thriller/Sci-Fi'
            WHEN categories ILIKE '%Family%' OR categories ILIKE '%Children%' THEN 'Family'
            ELSE 'Other Genres'
        END AS genre_group,
        
        -- 메타데이터
        last_update AS source_last_update,
        CURRENT_TIMESTAMP AS loaded_at
        
    FROM {{ ref('stg_dvdrental__films') }}
)

SELECT * FROM films_enriched
