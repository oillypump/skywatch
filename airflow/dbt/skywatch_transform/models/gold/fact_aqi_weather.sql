{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    type='iceberg',
    properties={
        "partitioning": "ARRAY['city_id'] ",
        "sorted_by": "ARRAY['date_id']"
    }
) }}


with incremental_threshold as (
    {% if is_incremental() %}
    select max(scraped_ts) - interval '2' day as limit_ts from {{ this }}
    {% else %}
    select timestamp '2020-01-01 00:00:00' as limit_ts
    {% endif %}
),

source as (
    select 
    	coalesce(a.province ,w.province ) as province ,
    	coalesce(a.city , w.city ) as city,
    	coalesce(w.forecast_ts, a.event_ts ) as event_ts,
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
    cross join incremental_threshold it
    where coalesce(w.forecast_ts, a.event_ts) >= it.limit_ts
),
deduplicated as (
	select *,
		row_number() over (
		partition by city, event_ts
		order by scraped_ts ) as rn
	from source
)
select
	to_hex(md5(to_utf8(s.city|| cast(s.event_ts as varchar)))) as id,
    to_hex(md5(to_utf8(city))) as city_id,
	to_hex(md5(to_utf8(aqi_status))) as aqi_id,
	CAST(format_datetime(event_ts, 'yyyyMMddHH') AS BIGINT) as date_id,
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
WHERE s.rn = 1
ORDER BY event_ts DESC