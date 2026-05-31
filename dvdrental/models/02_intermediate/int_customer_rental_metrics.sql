-- models/02_intermediate/int_customer_rental_metrics.sql
{{ config(
    materialized='view',
    schema='intermediate'
) }}

WITH rentals AS (
    SELECT * FROM {{ ref('stg_dvdrental__rentals') }}
),

payments AS (
    SELECT * FROM {{ ref('stg_dvdrental__payments') }}
),

customer_rental_metrics AS (
    SELECT
        rentals.customer_id,
        COUNT(DISTINCT rentals.rental_id) AS total_rentals,
        COUNT(DISTINCT rentals.film_id) AS distinct_films_rented,
        MIN(rentals.rental_date) AS first_rental_at,
        MAX(rentals.rental_date) AS last_rental_at,
        AVG(rentals.actual_rental_duration_days) AS avg_rental_duration_days,
        COUNT(*) FILTER (WHERE rentals.is_currently_rented) AS current_open_rentals,
        COALESCE(SUM(payments.amount), 0) AS total_payment_amount,
        COALESCE(AVG(payments.amount), 0) AS avg_payment_amount,
        MAX(payments.payment_date) AS last_payment_at
    FROM rentals
    LEFT JOIN payments ON rentals.rental_id = payments.rental_id
    GROUP BY rentals.customer_id
)

SELECT * FROM customer_rental_metrics
