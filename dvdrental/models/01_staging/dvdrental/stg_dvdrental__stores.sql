-- models/01_staging/dvdrental/stg_dvdrental__stores.sql
{{ config(
    materialized='view',
    schema='staging'
) }}

WITH store_source AS (
    SELECT * FROM {{ source('dvdrental', 'store') }}
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
        s.store_id,
        s.manager_staff_id,
        s.last_update,
        a.address,
        a.district,
        a.postal_code,
        c.city,
        co.country,
        {{ dbt_utils.generate_surrogate_key(['s.store_id']) }} AS store_key
    FROM store_source s
    LEFT JOIN address_source a ON s.address_id = a.address_id
    LEFT JOIN city_source c ON a.city_id = c.city_id
    LEFT JOIN country_source co ON c.country_id = co.country_id
)

SELECT * FROM cleaned
