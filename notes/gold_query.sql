with
    source as (
        SELECT
            COALESCE(a.province, f.province) as province,
            COALESCE(a.city, f.city) as city,
            COALESCE(a.event_ts, f.forecast_ts) as event_ts,
            a.aqi_val as act_aqi,
            f.aqi_val as fc_aqi,
            a.aqi_status as aqi_status,
            a.main_pollutant as main_pollutant,
            a.pollutant_val as act_pollutant_val,
            a.weather_condition as act_weather_condition,
            f.weather_condition as fc_weather_condition,
            a.temp_val as act_temp_val,
            f.temp_val as fc_temp_val,
            a.humidity_val as act_humdiy_val,
            f.humidity_val as fc_humidity_val,
            a.wind_val as act_wind_val,
            f.wind_val as fc_wind_val,
            a.wind_dir,
            a.alert,
            coalesce(a.scraped_ts, f.scraped_ts) as scraped_ts,
            f.observation_ts
        FROM
            iceberg.silver.aqi_index a
            FULL OUTER JOIN iceberg.silver.forecast_weather f ON a.city = f.city
            AND a.event_ts = f.forecast_ts
        WHERE
            COALESCE(a.event_ts, f.forecast_ts) >= CURRENT_DATE - INTERVAL '1' DAY
    )
SELECT
    to_hex (md5 (to_utf8 (city || cast(event_ts as varchar)))) as fact_id,
    *
FROM
    source
where
    city = 'depok'
ORDER BY
    event_ts DESC