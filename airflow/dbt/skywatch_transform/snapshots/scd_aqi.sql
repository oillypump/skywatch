{% snapshot scd_aqi %}

{{
    config(
      target_schema='silver',
      strategy='check',
      unique_key='id',
      check_cols=['category'],
      invalidate_hard_deletes=True,
    )
}}

{# 
   Logic: 
   1. Kita mengambil data unik city & province dari silver_aqi_index.
   2. 'strategy=check' artinya dbt akan memantau kolom city & province.
   3. Jika ada perubahan pada kolom tersebut, dbt otomatis membuat record baru (SCD2).
#}

WITH source_data AS (
    SELECT
        to_hex(md5(to_utf8(aqi_status))) as id,
        aqi_status as category,
        MIN(aqi_val) as min_val,
        MAX(aqi_val) as max_val
    FROM {{ ref('aqi_index') }}
    GROUP BY aqi_status
)

SELECT 
    id,
    category,
    min_val,
    max_val
FROM source_data

{% endsnapshot %}