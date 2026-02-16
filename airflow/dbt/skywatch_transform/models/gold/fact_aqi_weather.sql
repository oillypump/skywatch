{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='fact_id',
    type='iceberg',
    partitioning=['city_id', 'day(event_ts)'],
    sorted_by=['event_ts']
) }}

WITH source_data AS (
    SELECT 
        COALESCE(a.city, f.city) as city,       
        COALESCE(a.event_ts, f.forecast_ts) as event_ts,
        a.aqi_val as act_aqi,
        f.aqi_val as fc_aqi,
        a.aqi_status,
        a.main_pollutant, 
        a.pollutant_val,
        a.weather_condition as act_weather_condition,
        f.weather_condition as fc_weather_condition,
        a.temp_val as act_temp_val,
        f.temp_val as fc_temp_val,
        a.humidity_val as act_humidity_val,
        f.humidity_val as fc_humidity_val,
        a.wind_val as act_wind_val,
        f.wind_val as fc_wind_val,
        a.wind_dir,
        a.alert,
        COALESCE(a.scraped_ts, f.scraped_ts) as scraped_ts,
        f.observation_ts 
    FROM {{ source('silver', 'aqi_index') }} a
    FULL OUTER JOIN {{ source('silver', 'forecast_weather') }} f
        ON a.city = f.city AND a.event_ts = f.forecast_ts
    
    {% if is_incremental() %}
    WHERE COALESCE(a.event_ts, f.forecast_ts) >= CURRENT_DATE - INTERVAL '1' DAY
    {% endif %}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY city, event_ts 
            ORDER BY scraped_ts DESC
        ) as rn
    FROM source_data
)

SELECT 
    to_hex(md5(to_utf8(s.city || cast(s.event_ts as varchar)))) as fact_id,
    s.event_ts,
    c.id as city_id,
    COALESCE(s.act_aqi, s.fc_aqi) as aqi,
    a.id as aqi_id,
    COALESCE(s.act_weather_condition, s.fc_weather_condition) as weather_condition,
    COALESCE(s.act_temp_val, s.fc_temp_val) as temp_val,
    COALESCE(s.act_humidity_val, s.fc_humidity_val) as humidity_val,
    COALESCE(s.act_wind_val, s.fc_wind_val) as wind_val,
    s.pollutant_val,
    s.main_pollutant,
    s.wind_dir,
    s.alert,
    s.scraped_ts as load_ts,
    s.observation_ts 
FROM deduplicated s
LEFT JOIN {{ ref('dim_city') }} c
    ON s.city = c.city
LEFT JOIN {{ ref('dim_aqi') }} a
    ON COALESCE(s.act_aqi, s.fc_aqi) BETWEEN a.min_val AND a.max_val
WHERE s.rn = 1