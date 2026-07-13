{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    type='iceberg',
    properties={
        "partitioning": "ARRAY['year_val']"
    }
) }}

WITH source_data AS (
    SELECT 
        full_date_time,
        scraped_ts
    FROM (
        SELECT 
            COALESCE(w.forecast_ts, a.event_ts) as full_date_time,
            COALESCE(w.scraped_ts, a.scraped_ts) as scraped_ts,
            ROW_NUMBER() OVER (
                PARTITION BY COALESCE(w.forecast_ts, a.event_ts) 
                ORDER BY COALESCE(w.scraped_ts, a.scraped_ts) DESC
            ) as rn
        FROM {{ ref('aqi_index') }} a
        FULL OUTER JOIN {{ ref('weather_forecast') }} w
            ON a.city = w.city AND a.event_ts = w.forecast_ts
        WHERE COALESCE(w.forecast_ts, a.event_ts) IS NOT NULL
        
        {% if is_incremental() %}
        AND COALESCE(w.scraped_ts, a.scraped_ts) >= (SELECT max(scraped_ts) - interval '2' day FROM {{ ref('aqi_index') }})
        {% endif %}
    )
    WHERE rn = 1 
),

final_source AS (
    SELECT
        CAST(format_datetime(full_date_time, 'yyyyMMddHH') AS BIGINT) as id,
        full_date_time,
        CAST(full_date_time AS DATE) as full_date,
        year(full_date_time) as year_val,
        quarter(full_date_time) as quarter_val,
        'Q' || CAST(quarter(full_date_time) AS VARCHAR) as quarter_name,
        CASE WHEN month(full_date_time) <= 6 THEN 1 ELSE 2 END as semester_val,
        CASE WHEN month(full_date_time) <= 6 THEN 'S1' ELSE 'S2' END as semester_name,
        month(full_date_time) as month_val,
        format_datetime(full_date_time, 'MMMM') as month_name,
        week(full_date_time) as week_of_year,
        CAST(FLOOR((day(full_date_time) - 1) / 7) + 1 AS INTEGER) as week_of_month,
        day(full_date_time) as day_val,
        format_datetime(full_date_time, 'EEEE') as day_name,
        hour(full_date_time) as hour_val,
        CASE WHEN day_of_week(full_date_time) >= 6 THEN true ELSE false END as is_weekend
    FROM source_data
)

SELECT 
    s.*,
    {% if is_incremental() %}
        COALESCE(t.created_at, CAST(current_timestamp AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP)) as created_at,
    {% else %}
        CAST(current_timestamp AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP) as created_at,
    {% endif %}
    CAST(current_timestamp AT TIME ZONE 'Asia/Jakarta' AS TIMESTAMP) as updated_at
FROM final_source s
{% if is_incremental() %}
    LEFT JOIN {{ this }} t ON s.id = t.id
{% endif %}