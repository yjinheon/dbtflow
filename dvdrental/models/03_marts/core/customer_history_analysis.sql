-- models/marts/core/customer_history_analysis.sql
{{ config(
    materialized='view',
    schema='marts'
) }}

WITH customer_versions AS (
    SELECT
        customer_business_key,
        effective_start_date,
        effective_end_date,
        is_current_record,
        email,
        address,
        is_active,
        LAG(email) OVER (
            PARTITION BY customer_business_key
            ORDER BY effective_start_date
        ) AS previous_email,
        LAG(address) OVER (
            PARTITION BY customer_business_key
            ORDER BY effective_start_date
        ) AS previous_address,
        LAG(is_active) OVER (
            PARTITION BY customer_business_key
            ORDER BY effective_start_date
        ) AS previous_is_active
    FROM {{ ref('dim_customers') }}
),

customer_changes AS (
    SELECT
        customer_business_key,
        COUNT(*) AS total_changes,
        MIN(effective_start_date) AS first_record_date,
        MAX(effective_end_date) AS last_record_date,
        COUNT(CASE WHEN is_current_record THEN 1 END) AS current_records,

        -- 변경 유형 분석
        COUNT(CASE WHEN previous_email IS DISTINCT FROM email THEN 1 END) AS email_changes,
        COUNT(CASE WHEN previous_address IS DISTINCT FROM address THEN 1 END) AS address_changes,
        COUNT(CASE WHEN previous_is_active IS DISTINCT FROM is_active THEN 1 END) AS status_changes

    FROM customer_versions
    GROUP BY customer_business_key
),

customer_segments AS (
    SELECT
        *,
        CASE
            WHEN total_changes = 1 THEN 'Stable Customer'
            WHEN total_changes BETWEEN 2 AND 3 THEN 'Moderate Changes'
            WHEN total_changes > 3 THEN 'Frequent Changes'
        END AS change_pattern
    FROM customer_changes
)

SELECT * FROM customer_segments
