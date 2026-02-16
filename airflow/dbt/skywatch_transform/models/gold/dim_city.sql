{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    type='iceberg',
) }}


WITH raw_data AS (
    SELECT DISTINCT city, province 
    FROM {{ source('silver', 'aqi_index') }}
    {% if is_incremental() %}
    WHERE scraped_ts >= CURRENT_DATE - INTERVAL '2' DAY
    {% endif %}
)

SELECT 
    to_hex(md5(to_utf8(city))) as id,
    city,
    province,
    'Indonesia' as country,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) as created_ts,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) as updated_ts
FROM raw_data