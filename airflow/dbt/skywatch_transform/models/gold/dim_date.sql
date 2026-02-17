{{ config(
    materialized='table',
    type='iceberg'
) }}

SELECT
    id,
    full_date_time,
    full_date,
    is_current_month,
    is_current_year,
    is_current_day,
    year_val,
    quarter_val,
    quarter_name,
    semester_val,
    semester_name,
    month_val,
    month_name,
    week_of_year,
    week_of_month,
    day_val,
    day_name,
    hour_val,
    is_weekend,
    dbt_valid_from AS created_at,
    dbt_updated_at AS updated_at
FROM {{ ref('scd_date') }}
WHERE dbt_valid_to IS NULL