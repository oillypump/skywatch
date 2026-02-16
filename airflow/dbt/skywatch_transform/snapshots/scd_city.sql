{% snapshot scd_city %}

{{
    config(
      target_schema='silver',
      strategy='check',
      unique_key='id',
      check_cols=['city', 'province'],
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
    SELECT DISTINCT 
        LOWER(TRIM(city)) as city, 
        LOWER(TRIM(province)) as province 
    FROM {{ ref('aqi_index') }} 
)

SELECT 
    to_hex(md5(to_utf8(city))) as id,
    city,
    province,
    'Indonesia' as country
FROM source_data

{% endsnapshot %}