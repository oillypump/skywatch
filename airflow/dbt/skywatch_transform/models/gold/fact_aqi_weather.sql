{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    type='iceberg',
    properties={
        "partitioning": "ARRAY['day(event_ts)']",
        "sorted_by": "ARRAY['city_id', 'event_ts']"
    }
) }}

with source as (
    select 
    	coalesce(a.province ,w.province ) as province ,
    	coalesce(a.city , w.city ) as city,
    	coalesce(w.forecast_ts, a.event_ts ) as event_nih,
    	a.aqi_val as act_aqi,
        w.aqi_val as fc_aqi,
        a.aqi_status,
        a.main_pollutant, 
        a.pollutant_val,
        a.weather_condition as act_weather_condition,
        w.weather_condition as fc_weather_condition,
        a.temp_val as act_temp_val,
        w.temp_val as fc_temp_val,
        a.humidity_val as act_humidity_val,
        w.humidity_val as fc_humidity_val,
        a.wind_val as act_wind_val,
        w.wind_val as fc_wind_val,
        a.wind_dir,
        a.alert,
        COALESCE(w.scraped_ts, a.scraped_ts) as scraped_ts,
        w.observation_ts
    from  {{ source('silver', 'aqi_index') }} a
    full outer join {{ source('silver', 'weather_forecast') }} w
    on a.city = w.city AND a.event_ts = w.forecast_ts 
    {% if is_incremental() %}
    WHERE coalesce(w.forecast_ts, a.event_ts) >= (SELECT MAX(event_ts) - INTERVAL '2' DAY FROM {{ this }})
    {% endif %}
),
deduplicated as (
	select *,
		row_number() over (
		partition by city, event_nih
		order by scraped_ts ) as rn
	from source
)
select
	to_hex(md5(to_utf8(s.city|| cast(s.event_nih as varchar)))) as id,
	event_nih as event_ts,
	c.id as city_id,
	a.id as aqi_id,
	coalesce(s.fc_aqi,s.act_aqi) as aqi,
	coalesce(s.fc_weather_condition ,s.act_weather_condition ) as weather_condition,
	COALESCE(s.fc_temp_val, s.act_temp_val) as temp_val,
	coalesce(s.fc_humidity_val, s.act_humidity_val ) as humidity_val,
    coalesce(s.fc_wind_val , s.act_wind_val ) as wind_val,
    s.pollutant_val,
    s.main_pollutant,
    s.wind_dir,
    s.alert,
    s.scraped_ts as scraped_ts,
    s.observation_ts 
from deduplicated s
left join {{ ref('dim_aqi') }} a
	on coalesce(s.fc_aqi,s.act_aqi) BETWEEN a.min_val AND a.max_val
left join {{ ref('dim_city') }} c
	on s.city = c.city 
WHERE s.rn = 1
ORDER BY event_ts DESC