select
    *
from
    iceberg.bronze.raw_aqi_index;

select
    to_hex (
        md5 (
            to_utf8 (
                LOWER(TRIM(city)) || CAST(observation_time AS VARCHAR)
            )
        )
    ) as record_id,
    lower(trim(city)) as city,
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
    ) as observation_ts,
    CAST(scraped_at AS TIMESTAMP) as scraped_at_ts,
    'aqi_index' as data_source
from
    iceberg.bronze.raw_aqi_index
order by
    scraped_at_ts desc;

select
    --	to_hex(md5(to_utf8(LOWER(TRIM(city)) || CAST(observation_time AS VARCHAR)))) as record_id,
    to_hex (
        md5 (
            to_utf8 (LOWER(TRIM(city)) || CAST(forecast_ts AS VARCHAR))
        )
    ) as record_id2,
    to_hex (
        md5 (
            to_utf8 (
                LOWER(TRIM(city)) || CAST(observation_time AS VARCHAR) || CAST(forecast_ts AS VARCHAR)
            )
        )
    ) as record_id3,
    lower(trim(city)) as city,
    cast(forecast_ts as TIMESTAMP) as forecast_ts,
    aqi,
    weather,
    temperature as temp_c,
    wind_speed,
    humidity,
    CAST(
        parse_datetime (
            observation_time || ', 2026',
            'HH:mm, MMM dd, yyyy'
        ) AS TIMESTAMP
    ) as observation_ts,
    CAST(scraped_at AS TIMESTAMP) as scraped_at_ts
from
    iceberg.bronze.raw_weather_forecast
where
    city = 'Jakarta'
    --and cast(forecast_ts AS TIMESTAMP) = TIMESTAMP '2026-02-06 11:00:00'
    and forecast_ts = '2026-02-09 10:00:00'
order by
    observation_ts,
    forecast_ts asc;

SELECT
    *
FROM
    (
        SELECT
            to_hex (
                md5 (
                    to_utf8 (LOWER(TRIM(city)) || CAST(forecast_ts AS VARCHAR))
                )
            ) as record_id,
            city,
            CAST(forecast_ts AS TIMESTAMP) as forecast_ts,
            aqi,
            weather,
            temperature,
            -- Kita urutkan berdasarkan waktu scrape terbaru
            ROW_NUMBER() OVER (
                PARTITION BY
                    city,
                    forecast_ts
                ORDER BY
                    scraped_at DESC
            ) as rn
        FROM
            iceberg.bronze.raw_weather_forecast
        WHERE
            city = 'Jakarta'
            AND forecast_ts = '2026-02-09 10:00:00'
    )
WHERE
    rn = 1;



=================================================================================================================
WITH unified_data AS (
    -- AQI: Data Aktual
    SELECT 
        to_hex(md5(to_utf8(LOWER(TRIM(city)) || CAST(observation_time AS VARCHAR)))) as record_id,
        LOWER(TRIM(city)) as city,
        CAST(aqi AS INTEGER) as aqi,
        aqi_status,
        main_pollutant,
        concentration,
        weather,
        -- Cleansing: Hapus semua kecuali angka, titik, dan minus
        CAST(regexp_replace(temperature, '[^\d.-]', '') AS DOUBLE) as temp_c,
        CAST(regexp_replace(humidity, '[^\d.-]', '') AS INTEGER) as humidity_pct,
        CAST(regexp_replace(wind_speed, '[^\d.-]', '') AS DOUBLE) as wind_speed_kmh,
        wind_direction,
        alert,
        CAST(parse_datetime(observation_time || ', 2026', 'HH:mm, MMM dd, yyyy') AS TIMESTAMP) as event_ts,
        CAST(scraped_at AS TIMESTAMP) as scraped_ts,
        'aqi_index' as data_source
    FROM iceberg.bronze.raw_aqi_index

    UNION ALL

    -- FORECAST: Data Ramalan
    SELECT 
        to_hex(md5(to_utf8(LOWER(TRIM(city)) || CAST(forecast_ts AS VARCHAR)))) as record_id,
        LOWER(TRIM(city)) as city,
        CAST(aqi AS INTEGER) as aqi,
        CAST(NULL AS VARCHAR) as aqi_status,
        CAST(NULL AS VARCHAR) as main_pollutant,
        CAST(NULL AS VARCHAR) as concentration,
        weather,
        -- Cleansing yang sama untuk Forecast
        CAST(regexp_replace(temperature, '[^\d.-]', '') AS DOUBLE) as temp_c,
        CAST(regexp_replace(humidity, '[^\d.-]', '') AS INTEGER) as humidity_pct,
        CAST(regexp_replace(wind_speed, '[^\d.-]', '') AS DOUBLE) as wind_speed_kmh,
        CAST(NULL AS VARCHAR) as wind_direction,
        CAST(NULL AS VARCHAR) as alert,
        CAST(forecast_ts as TIMESTAMP) as event_ts,
        CAST(scraped_at AS TIMESTAMP) as scraped_ts,
        'forecast_weather' as data_source
    FROM iceberg.bronze.raw_weather_forecast
),
final_deduplication AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city, event_ts 
            ORDER BY 
                CASE WHEN data_source = 'aqi_index' THEN 1 ELSE 2 END ASC,
                scraped_ts DESC
        ) as rn
    FROM unified_data
)
SELECT 
    to_hex(md5(to_utf8(city || CAST(event_ts AS VARCHAR)))) as silver_record_id,
    city,
    event_ts,
    aqi,
    aqi_status,
    main_pollutant,
    concentration,
    weather,
    temp_c,
    humidity_pct,
    wind_speed_kmh,
    wind_direction,
    alert,
    scraped_ts,
    data_source
FROM final_deduplication 
WHERE rn = 1
and city = 'depok'
ORDER BY event_ts ASC;



select * from iceberg.bronze.raw_aqi_index  
where city = 'Depok'
order by observation_time ;

select * from iceberg.silver.weather_aqi_hourly 
where city = 'depok'
order by event_ts asc;

B0113C9810E088ECEBB68152C51E5EF3
52F131821AC0568F48828E471B2B505F

============================================================================================================
WITH unified_data AS (
	-- AQI
	select 
		to_hex(md5(to_utf8(LOWER(TRIM(city)) || CAST(observation_time AS VARCHAR)))) as record_id,
		lower(trim(city)) as city,
--		CAST(parse_datetime(observation_time || ', 2026', 'HH:mm, MMM dd, yyyy') AS TIMESTAMP) as forecast_ts,
		CAST(NULL AS TIMESTAMP) as forecast_ts,
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
		CAST(parse_datetime(observation_time || ', 2026', 'HH:mm, MMM dd, yyyy') AS TIMESTAMP) as observation_ts,
		CAST(scraped_at AS TIMESTAMP) as scraped_at_ts,
		'aqi_index' as data_source
	from iceberg.bronze.raw_aqi_index
	union all
	-- FORECAST WEATHER
	select 
		to_hex(md5(to_utf8(LOWER(TRIM(city)) || CAST(observation_time AS VARCHAR)))) as record_id,
		lower(trim(city)) as city,
		cast(forecast_ts as TIMESTAMP) as forecast_ts,
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
		CAST(parse_datetime(observation_time || ', 2026', 'HH:mm, MMM dd, yyyy') AS TIMESTAMP) as observation_ts,
		CAST(scraped_at AS TIMESTAMP) as scraped_at_ts,
		'forecast_weather' as datasource
	from iceberg.bronze.raw_weather_forecast
)
SELECT * FROM (
	SELECT *
--		,ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY scraped_at_ts DESC) as rn
	FROM unified_data
)
where 
--city = 'jakarta' 
--and 
record_id = 'EA526707807AF31FA7702F65ECD0DAE6'
order by observation_ts desc

;
WHERE rn = 1;


============================================================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.silver 
WITH (location = 's3a://lakehouse/silver/');


CREATE TABLE IF NOT EXISTS iceberg.silver.unified_weather_aqi (
                silver_record_id VARCHAR,
                city VARCHAR,
                event_ts TIMESTAMP(6),
                event_date DATE,
                event_hour INTEGER,
                aqi INTEGER,
                aqi_category VARCHAR,
                weather VARCHAR,
                temp_c DOUBLE,
                humidity_pct INTEGER,
                wind_speed_kmh DOUBLE,
                data_source VARCHAR,
                scraped_ts TIMESTAMP(6)
            )
            WITH (
                partitioning = ARRAY['city', 'month(event_ts)'],
                location = 's3a://lakehouse/silver/unified_weather_aqi',
                format = 'PARQUET'
            )
            
SHOW CREATE TABLE iceberg.silver.unified_weather_aqi;


INSERT INTO iceberg.silver.weather_aqi_hourly 
VALUES (
    'test-id-123', 
    'tangerang', 
    TIMESTAMP '2026-02-06 12:00:00', 
    DATE '2026-02-06', 
    12, 
    75, 
    'Moderate', 
    'Cloudy', 
    30.5, 
    80, 
    15.0, 
    'manual_test', 
    current_timestamp
);


SELECT * FROM iceberg.silver.unified_weather_aqi LIMIT 10;

DROP TABLE iceberg.silver.unified_weather_aqi;
DROP TABLE iceberg.silver.weather_aqi_hourly;

DROP SCHEMA IF EXISTS iceberg.silver;
============================================================================================================
WITH unified_data AS (
    -- SOURCE 1: AQI INDEX (REAL-TIME/HISTORICAL)
    SELECT 
        to_hex(md5(to_utf8(LOWER(TRIM(city)) || CAST(observation_time AS VARCHAR)))) as record_id,
        LOWER(TRIM(city)) as city,
        CAST(NULL AS TIMESTAMP) as forecast_ts, 
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
        CAST(parse_datetime(observation_time || ', 2026', 'HH:mm, MMM dd, yyyy') AS TIMESTAMP) as observation_ts,
        CAST(scraped_at AS TIMESTAMP) as scraped_at_ts,
        'aqi_index' as data_source
    FROM iceberg.bronze.raw_aqi_index

    UNION ALL

    -- SOURCE 2: WEATHER FORECAST
    SELECT 
        to_hex(md5(to_utf8(LOWER(TRIM(city)) || CAST(observation_time AS VARCHAR)))) as record_id,
        LOWER(TRIM(city)) as city,
        CAST(forecast_ts AS TIMESTAMP) as forecast_ts,
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
        CAST(parse_datetime(observation_time || ', 2026', 'HH:mm, MMM dd, yyyy') AS TIMESTAMP) as observation_ts,
        CAST(scraped_at AS TIMESTAMP) as scraped_at_ts,
        'forecast_weather' as data_source
    FROM iceberg.bronze.raw_weather_forecast
),
ranked_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city, observation_ts 
            ORDER BY 
                -- 1. Utamakan data Aktual (aqi_index) dibanding Forecast
                CASE WHEN data_source = 'aqi_index' THEN 1 ELSE 2 END ASC,
                -- 2. Ambil data hasil scrape terbaru jika ada ID yang sama
                scraped_at_ts DESC
        ) as rn
    FROM unified_data
)
-- FINAL OUTPUT
SELECT 
    record_id,
    city,
    observation_ts,
    forecast_ts,
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
    scraped_at_ts,
    data_source
FROM ranked_data
WHERE rn = 1
and 
city = 'depok'
ORDER BY city, observation_ts ASC;


============================================================================================================
WITH unified_data AS (
	SELECT city, 'aqi_index' as data_source FROM iceberg.bronze.raw_aqi_index
	UNION ALL
	SELECT city, 'forecast_weather' as data_source FROM iceberg.bronze.raw_weather_forecast
)
SELECT count(1) FROM (
	SELECT *
	FROM unified_data
) where city = 'Jakarta'



============================================================================================================
WITH unified_data AS (
	-- AQI
	select 
		to_hex(md5(to_utf8(LOWER(TRIM(city)) || CAST(observation_time AS VARCHAR)))) as record_id,
		lower(trim(city)) as city,
		CAST(parse_datetime(observation_time || ', 2026', 'HH:mm, MMM dd, yyyy') AS TIMESTAMP) as forecast_ts,
--		CAST(NULL AS TIMESTAMP) as forecast_ts,
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
		CAST(parse_datetime(observation_time || ', 2026', 'HH:mm, MMM dd, yyyy') AS TIMESTAMP) as observation_ts,
		CAST(scraped_at AS TIMESTAMP) as scraped_at_ts,
		'aqi_index' as data_source
	from iceberg.bronze.raw_aqi_index
	union all
	-- FORECAST WEATHER
	select 
		to_hex(md5(to_utf8(LOWER(TRIM(city)) || CAST(forecast_ts AS VARCHAR)))) as record_id,
		lower(trim(city)) as city,
		cast(forecast_ts as TIMESTAMP) as forecast_ts,
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
		CAST(parse_datetime(observation_time || ', 2026', 'HH:mm, MMM dd, yyyy') AS TIMESTAMP) as observation_ts,
		CAST(scraped_at AS TIMESTAMP) as scraped_at_ts,
		'forecast_weather' as datasource
	from iceberg.bronze.raw_weather_forecast
)
select
	*
FROM (
	SELECT *,
	ROW_NUMBER() OVER (
            PARTITION BY city, observation_ts 
            ORDER BY 
                -- PRIORITAS 1: Pakai data asli (aqi_index) jika tersedia
                CASE WHEN data_source = 'aqi_index' THEN 1 ELSE 2 END ASC,
                -- PRIORITAS 2: Jika sama-sama aqi_index atau sama-sama forecast, ambil yang paling baru di-scrape
                scraped_at_ts DESC
        ) as rn
	FROM unified_data
);


WHERE 
--record_id = 'EA526707807AF31FA7702F65ECD0DAE6'
city = 'jakarta'
order by observation_ts asc;

--and 

--where rn = 1;

============================================================================================================
============================================================================================================
============================================================================================================