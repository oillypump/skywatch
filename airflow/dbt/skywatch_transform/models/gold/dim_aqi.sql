{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    type='iceberg'
) }}

SELECT
    to_hex(md5(to_utf8(aqi_status))) as id,
    aqi_status as category,
    MIN(aqi_val) as min_val,
    MAX(aqi_val) as max_val,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) as created_ts,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) as updated_ts 
FROM {{ source('silver', 'aqi_index') }}

{% if is_incremental() %}
-- Filter diletakkan di sini agar scanning data lebih sedikit
WHERE scraped_ts >= CURRENT_DATE - INTERVAL '2' DAY
{% endif %}

GROUP BY aqi_status