{{ config(
    unique_key='id',
    type='iceberg'
) }}


WITH source_data AS (
    SELECT
        to_hex(md5(to_utf8(aqi_status))) as id,
        aqi_status as category,
        MIN(aqi_val) as min_val,
        MAX(aqi_val) as max_val
    FROM {{ ref('aqi_index') }}
    GROUP BY aqi_status
), 

final_source AS (
    SELECT 
        id,
        category,
        min_val,
        max_val
    FROM source_data
)

SELECT 
    s.id,
    s.category,
    s.min_val,
    s.max_val,
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