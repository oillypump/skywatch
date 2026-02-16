{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    type='iceberg',
    properties={
        "partitioning": "ARRAY['city', 'day(event_ts)']",
        "sorted_by": "ARRAY['event_ts']"
    }
) }}

WITH cleaned_source AS (
    SELECT 
        LOWER(TRIM(province)) as province ,
        LOWER(TRIM(city)) as city,
        CAST(
            parse_datetime(
                observation_time || ', ' || CAST(year(CAST(scraped_at AS TIMESTAMP)) AS VARCHAR), 
                'HH:mm, MMM dd, yyyy'
            ) AS TIMESTAMP
        ) as event_ts,                    
        CAST(regexp_replace(aqi, '[^\d]', '') AS INTEGER) as aqi_val,
        aqi_status,
        main_pollutant,                    
        CAST(regexp_replace(concentration, '[^\d.-]', '') AS DOUBLE) as pollutant_val,
        weather as weather_condition,
        CAST(regexp_replace(temperature, '[^\d.-]', '') AS DOUBLE) as temp_val,
        CAST(regexp_replace(humidity, '[^\d.-]', '') AS INTEGER) as humidity_val,
        CAST(regexp_replace(wind_speed, '[^\d.-]', '') AS DOUBLE) as wind_val,        
        wind_direction as wind_dir,
        alert as alert,
        CAST(scraped_at AS TIMESTAMP) as scraped_ts
    FROM {{ source('bronze', 'raw_aqi_index') }}
),
deduplicated AS (
    SELECT *,
        to_hex(md5(to_utf8(city || CAST(event_ts AS VARCHAR)))) as id,
    	ROW_NUMBER() OVER (PARTITION BY city, event_ts ORDER BY scraped_ts DESC) as rn
    FROM cleaned_source
)
SELECT 
    id,
    province,
    city,
    event_ts,
    aqi_val,
    aqi_status,
    main_pollutant,
    pollutant_val,
    weather_condition,
    temp_val,
    humidity_val,
    wind_val,
    wind_dir,
    alert,
    scraped_ts
FROM deduplicated 
WHERE rn = 1