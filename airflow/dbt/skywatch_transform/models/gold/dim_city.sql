{{ config(
    materialized='table',
    type='iceberg'
) }}

SELECT
    id,
    city,
    province,
    country,
    dbt_valid_from AS created_at,
    dbt_updated_at AS updated_at
FROM {{ ref('scd_city') }}
WHERE dbt_valid_to IS NULL