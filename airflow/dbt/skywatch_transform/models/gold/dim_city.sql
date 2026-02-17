{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    type='iceberg'
) }}

WITH source_data AS (
    SELECT DISTINCT 
        LOWER(TRIM(city)) as city, 
        LOWER(TRIM(province)) as province 
    FROM {{ ref('aqi_index') }} 
),

final_source AS (
    SELECT 
        to_hex(md5(to_utf8(city))) as id,
        city,
        province,
        'Indonesia' as country
    FROM source_data
)

SELECT 
    s.id,
    s.city,
    s.province,
    s.country,
    -- created_at: Ambil dari tabel asli jika sudah ada, jika baru pakai waktu sekarang
    {% if is_incremental() %}
        COALESCE(t.created_at, CAST(current_timestamp AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)) as created_at,
    {% else %}
        CAST(current_timestamp AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP) as created_at,
    {% endif %}
    -- updated_at: Selalu diperbarui ke waktu sekarang setiap ada merge/run
    CAST(current_timestamp AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP) as updated_at
FROM final_source s
{% if is_incremental() %}
    LEFT JOIN {{ this }} t ON s.id = t.id
{% endif %}