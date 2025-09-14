-- models/marts/core/customer_history_analysis.sql
{{ config(
    materialized='view',
    schema='marts'
) }}

WITH customer_changes AS (
    SELECT
        customer_business_key,
        COUNT(*) AS total_changes,
        MIN(effective_start_date) AS first_record_date,
        MAX(effective_end_date) AS last_record_date,
        COUNT(CASE WHEN is_current_record THEN 1 END) AS current_records,

        -- 변경 유형 분석
        COUNT(
            CASE
                WHEN
                    LAG(email)
                        OVER (
                            PARTITION BY customer_business_key
                            ORDER BY effective_start_date
                        )
                    != email
                    THEN 1
            END
        ) AS email_changes,
        COUNT(
            CASE
                WHEN
                    LAG(address)
                        OVER (
                            PARTITION BY customer_business_key
                            ORDER BY effective_start_date
                        )
                    != address
                    THEN 1
            END
        ) AS address_changes,
        COUNT(
            CASE
                WHEN
                    LAG(is_active)
                        OVER (
                            PARTITION BY customer_business_key
                            ORDER BY effective_start_date
                        )
                    != is_active
                    THEN 1
            END
        ) AS status_changes

    FROM {{ ref('dim_customers') }}
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
