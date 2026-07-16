{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    merge_exclude_columns=['insert_ts'],
    tags=['aqi']
) }}

WITH source_data AS (
    SELECT * 
    FROM {{ source('BRONZE', 'AQI_RAW') }}
    WHERE observation_ts IS NOT NULL
    
    {% if is_incremental() %}
      -- Menyaring data mentah yang masuk hanya yang lebih baru dari data terakhir di silver
      AND scraped_ts > (SELECT max(scraped_ts) FROM {{ this }})
    {% endif %}
),

-- Langkah 1: Bersihkan data, buat ID unik (MD5), dan berikan penomoran baris untuk deduplikasi
cleaned_and_numbered AS (
    SELECT 
        md5(concat_ws('|', lower(province), lower(city), to_varchar(observation_ts))) AS id,
        lower(province) AS province,
        lower(city) AS city,
        try_to_number(regexp_replace(aqi, '[^0-9.]', ''), 10, 0) AS aqi_val,
        lower(aqi_status) AS aqi_status,
        main_pollutant,
        try_to_number(regexp_replace(concentration, '[^0-9.]', ''), 10, 2) AS concentration_val,
        lower(weather) AS weather,
        try_to_number(regexp_replace(temperature, '[^0-9.\-]', ''), 10, 1) AS temperature_val,
        try_to_number(regexp_replace(humidity, '[^0-9.]', ''), 10, 1) AS humidity_val,
        try_to_number(regexp_replace(wind_speed, '[^0-9.]', ''), 10, 1) AS wind_speed_val,
        lower(wind_direction) AS wind_direction,
        lower(alert) AS alert,
        observation_ts,
        scraped_ts,
        -- Kita kunci ORDER BY di level CTE ini agar tidak hilang saat dbt membungkus query untuk MERGE
        row_number() OVER (
            PARTITION BY province, city, observation_ts
            ORDER BY scraped_ts DESC
        ) AS rn
    FROM source_data
)

-- Langkah 2: Ambil record terbaru (rn = 1) tanpa melakukan window function lagi di query terluar
SELECT 
    id,
    province,
    city,
    aqi_val,
    aqi_status,
    main_pollutant,
    concentration_val,
    weather,
    temperature_val,
    humidity_val,
    wind_speed_val,
    wind_direction,
    alert,
    observation_ts,
    scraped_ts,
    current_timestamp()  as insert_ts,
    current_timestamp()  as update_ts
FROM cleaned_and_numbered
WHERE rn = 1