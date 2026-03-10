{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    type='iceberg',
    properties={
        "partitioning": "ARRAY['city', 'day(observation_ts)']",
        "sorted_by": "ARRAY['observation_ts']"
    }
) }}

WITH cleaned_source AS (
    SELECT 
        LOWER(TRIM(province)) as province,
        LOWER(TRIM(city)) as city,
		CAST(forecast_ts AS TIMESTAMP) as forecast_ts,
		CAST(parse_datetime(
            format_datetime(CAST(scraped_ts AS TIMESTAMP), 'yyyy-MM-dd') || ' ' || 
            split_part(observation_ts, ',', 1), 
            'yyyy-MM-dd HH:mm'
        ) AS TIMESTAMP) as observation_ts,
        CAST(aqi AS INTEGER) as aqi_val,
        weather as weather_condition,
        CAST(regexp_replace(temperature, '[^\d.-]', '') AS DOUBLE) as temp_val,
        CAST(regexp_replace(wind_speed, '[^\d.-]', '') AS DOUBLE) as wind_val,
        CAST(regexp_replace(humidity, '[^\d.-]', '') AS INTEGER) as humidity_val,
        CAST(scraped_ts AS TIMESTAMP) as scraped_ts
    FROM {{source('bronze', 'raw_weather_forecast') }}
    where observation_ts != 'N/A' and NOT (observation_ts LIKE 'Air%')
),
deduplicated AS (
    SELECT *,
        to_hex(md5(to_utf8(city || CAST(forecast_ts AS VARCHAR)))) as id,
        ROW_NUMBER() OVER (PARTITION BY city, forecast_ts ORDER BY scraped_ts DESC) as rn
    FROM cleaned_source
)
SELECT 
    id,
    province,
    city,
    forecast_ts,
    observation_ts,
    aqi_val,
    weather_condition,
    temp_val,
    wind_val,
    humidity_val,
    scraped_ts
FROM deduplicated 
WHERE rn = 1