{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH date_range AS (
    SELECT 
        generate_series(
            (SELECT MIN(DATE(message_date)) FROM {{ ref('stg_telegram_messages') }}),
            (SELECT MAX(DATE(message_date)) FROM {{ ref('stg_telegram_messages') }}),
            interval '1 day'
        )::date as full_date
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['full_date']) }} as date_key,
    full_date,
    EXTRACT(DAY FROM full_date) as day_of_month,
    EXTRACT(DOW FROM full_date) as day_of_week,
    TO_CHAR(full_date, 'Day') as day_name,
    EXTRACT(WEEK FROM full_date) as week_of_year,
    EXTRACT(MONTH FROM full_date) as month,
    TO_CHAR(full_date, 'Month') as month_name,
    EXTRACT(QUARTER FROM full_date) as quarter,
    EXTRACT(YEAR FROM full_date) as year,
    CASE 
        WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE
        ELSE FALSE
    END as is_weekend,
    CASE 
        WHEN EXTRACT(MONTH FROM full_date) IN (1, 2, 12) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM full_date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM full_date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Autumn'
    END as season
FROM date_range