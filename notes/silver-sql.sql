WITH
    unified_data AS (
        SELECT
            to_hex (
                md5 (
                    to_utf8 (
                        LOWER(TRIM(city)) || CAST(observation_time AS VARCHAR)
                    )
                )
            ) as record_id,
            LOWER(TRIM(city)) as city,
            aqi,
            aqi_status,
            main_pollutant,
            concentration,
            weather,
            temperature as temp_c,
            humidity,
            wind_speed,
            wind_direction,
            alert,
            CAST(
                parse_datetime (
                    observation_time || ', 2026',
                    'HH:mm, MMM dd, yyyy'
                ) AS TIMESTAMP
            ) as event_ts,
            CAST(scraped_at AS TIMESTAMP) as scraped_ts,
            'aqi_index' as data_source
        FROM
            iceberg.bronze.raw_aqi_index
        UNION ALL
        SELECT
            to_hex (
                md5 (
                    to_utf8 (LOWER(TRIM(city)) || CAST(forecast_ts AS VARCHAR))
                )
            ) as record_id,
            LOWER(TRIM(city)) as city,
            aqi,
            CAST(NULL AS VARCHAR) as aqi_status,
            CAST(NULL AS VARCHAR) as main_pollutant,
            CAST(NULL AS VARCHAR) as concentration,
            weather,
            temperature as temp_c,
            humidity,
            wind_speed,
            CAST(NULL AS VARCHAR) as wind_direction,
            CAST(NULL AS VARCHAR) as alert,
            CAST(forecast_ts as TIMESTAMP) as event_ts,
            CAST(scraped_at AS TIMESTAMP) as scraped_ts,
            'forecast_weather' as data_source
        FROM
            iceberg.bronze.raw_weather_forecast
    ),
    final_deduplication AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    city,
                    event_ts
                ORDER BY
                    CASE
                        WHEN data_source = 'aqi_index' THEN 1
                        ELSE 2
                    END ASC,
                    scraped_ts DESC
            ) as rn
        FROM
            unified_data
    )
SELECT
    to_hex (md5 (to_utf8 (city || CAST(event_ts AS VARCHAR)))) as silver_record_id,
    --    record_id,
    city,
    event_ts,
    aqi,
    aqi_status,
    main_pollutant,
    concentration,
    weather,
    temp_c,
    humidity,
    wind_speed,
    wind_direction,
    alert,
    scraped_ts,
    data_source,
    rn
FROM
    final_deduplication
WHERE
    city = 'tangerang'
    and rn = 1
ORDER BY
    event_ts ASC;