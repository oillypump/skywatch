{{ config(
    materialized='table',
    type='iceberg',
    properties={
        "partitioning": "ARRAY['year_val']"
    }
) }}

WITH source_data AS (
    SELECT DISTINCT
        COALESCE(w.forecast_ts, a.event_ts) as full_date_time
    FROM {{ ref('aqi_index') }} a
    FULL OUTER JOIN {{ ref('weather_forecast') }} w
        ON a.city = w.city AND a.event_ts = w.forecast_ts
    WHERE COALESCE(w.forecast_ts, a.event_ts) IS NOT NULL
)
SELECT
    CAST(format_datetime(full_date_time, 'yyyyMMddHH') AS BIGINT) as id,
    full_date_time,
    CAST(full_date_time AS DATE) as full_date,

    -- PERBAIKAN: Gunakan current_timestamp agar bisa dipasang TIME ZONE
    CASE 
        WHEN year(full_date_time) = year(current_timestamp AT TIME ZONE 'Asia/Jakarta') THEN true 
        ELSE false 
    END as is_current_year,

    CASE 
        WHEN year(full_date_time) = year(current_timestamp AT TIME ZONE 'Asia/Jakarta') 
             AND month(full_date_time) = month(current_timestamp AT TIME ZONE 'Asia/Jakarta') THEN true 
        ELSE false 
    END as is_current_month,
    
    CASE 
        WHEN CAST(full_date_time AS DATE) = CAST(current_timestamp AT TIME ZONE 'Asia/Jakarta' AS DATE) THEN true 
        ELSE false 
    END as is_current_day,

    year(full_date_time) as year_val,
    quarter(full_date_time) as quarter_val,
    'Q' || CAST(quarter(full_date_time) AS VARCHAR) as quarter_name,
    
    CASE 
        WHEN month(full_date_time) <= 6 THEN 1 
        ELSE 2 
    END as semester_val,
    
    CASE 
        WHEN month(full_date_time) <= 6 THEN 'S1' 
        ELSE 'S2' 
    END as semester_name,
    
    month(full_date_time) as month_val,
    format_datetime(full_date_time, 'MMMM') as month_name,
    
    week(full_date_time) as week_of_year,
    CAST(FLOOR((day(full_date_time) - 1) / 7) + 1 AS INTEGER) as week_of_month,
    
    day(full_date_time) as day_val,
    format_datetime(full_date_time, 'EEEE') as day_name,
    hour(full_date_time) as hour_val,
    CASE 
        WHEN day_of_week(full_date_time) >= 6 THEN True
        ELSE False
    END as is_weekend
FROM source_data
ORDER BY full_date_time DESC