-- models/staging/dvdrental/stg_dvdrental__inventory.sql
{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source AS (
    SELECT * FROM {{ source('dvdrental', 'inventory') }}
)

SELECT
    inventory_id,
    film_id,
    store_id,
    last_update,
    
    -- 키 생성
    {{ dbt_utils.generate_surrogate_key(['inventory_id']) }} AS inventory_key,
    {{ dbt_utils.generate_surrogate_key(['film_id', 'store_id']) }} AS film_store_key
    
FROM source
