-- tests/assert_valid_scd_type2_customers.sql
-- SCD Type 2 구현의 데이터 품질을 검증하는 커스텀 테스트

WITH validation_checks AS (
    SELECT
        -- 1. 각 고객은 정확히 하나의 현재 레코드를 가져야 함
        customer_business_key,
        COUNT(CASE WHEN is_current_record THEN 1 END) AS current_record_count,

        -- 2. 날짜 범위가 겹치지 않아야 함
        COUNT(*) AS total_records,
        COUNT(DISTINCT effective_start_date) AS distinct_start_dates

    FROM {{ ref('dim_customers') }}
    GROUP BY customer_business_key
),

failed_validations AS (
    SELECT *
    FROM validation_checks
    WHERE
        current_record_count != 1  -- 현재 레코드가 정확히 1개가 아님
        OR total_records != distinct_start_dates  -- 시작 날짜 중복
)

SELECT * FROM failed_validations
