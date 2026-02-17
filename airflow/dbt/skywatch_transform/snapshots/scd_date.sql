{% snapshot scd_date %}

{{
    config(
      target_schema='silver',
      strategy='check',
      unique_key='id',
      check_cols='all',
      invalidate_hard_deletes=True,
    )
}}


WITH source_data AS (
    select distinct
		coalesce(w.forecast_ts, a.event_ts ) as full_date_time
    from {{ ref ('aqi_index')}} a
    full outer join {{ ref ('weather_forecast')}} w
    on a.city = w.city  AND a.event_ts = w.forecast_ts
    WHERE COALESCE(w.forecast_ts, a.event_ts) IS NOT NULL
)
select
	CAST(format_datetime(full_date_time, 'yyyyMMddHH') AS BIGINT) as id,
    full_date_time,
    CAST(full_date_time AS DATE) as full_date,
    CASE 
        WHEN year(full_date_time) = year(current_date) 
             AND month(full_date_time) = month(current_date) THEN true 
        ELSE false 
    END as is_current_month,
    
    CASE 
        WHEN year(full_date_time) = year(current_date) THEN true 
        ELSE false 
    END as is_current_year,
	CASE 
        WHEN day(full_date_time) = day(current_date) THEN true 
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
        WHEN day_of_week(full_date_time) <= 5 THEN False
        ELSE True
    END as is_weekend
FROM source_data
order by full_date_time desc

{% endsnapshot %}