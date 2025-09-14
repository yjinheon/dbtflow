-- analyses/staging_validation.sql
-- Staging 레이어 데이터 품질 검증

-- 1. 레코드 수 비교 정합성 체크 (원본 vs staging)
SELECT 
    'customers' as table_name,
    (SELECT COUNT(*) FROM {{ source('dvdrental', 'customer') }}) as source_count,
    (SELECT COUNT(*) FROM {{ ref('stg_dvdrental__customers') }}) as staging_count;

-- 2. 중복 키 체크
SELECT 
    'stg_customers' as model,
    COUNT(*) as total_records,
    COUNT(DISTINCT customer_key) as unique_keys,
    COUNT(*) - COUNT(DISTINCT customer_key) as duplicate_count
FROM {{ ref('stg_dvdrental__customers') }};

-- 3. NULL 분포 확인
SELECT 
    COUNT(CASE WHEN email IS NULL THEN 1 END) as null_emails,
    COUNT(CASE WHEN full_name IS NULL THEN 1 END) as null_names,
    COUNT(CASE WHEN region = 'Other' THEN 1 END) as other_regions
FROM {{ ref('stg_dvdrental__customers') }};
