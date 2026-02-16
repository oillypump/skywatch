{{ config(
    unique_key='id',
    type='iceberg'
) }}

SELECT 
    id,
    category,
    min_val,
    max_val,
    dbt_valid_from AS created_at,
    dbt_updated_at AS updated_at
FROM {{ ref('scd_aqi') }}
WHERE dbt_valid_to IS NULL