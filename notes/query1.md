with source as (
SELECT
COALESCE(a.province , f.province ) as province,
COALESCE(a.city, f.city) as city,
COALESCE(a.event_ts, f.forecast_ts) as event_ts,
a.aqi_val as act_aqi,
f.aqi_val as fc_aqi,
a.aqi_status as aqi_status,
a.main_pollutant as main_pollutant,
a.pollutant_val as pollutant_val,
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
coalesce(a.scraped_ts, f.scraped_ts ) as scraped_ts,
f.observation_ts
FROM iceberg.silver.aqi_index a
FULL OUTER JOIN iceberg.silver.forecast_weather f
ON a.city = f.city AND a.event_ts = f.forecast_ts
WHERE COALESCE(a.event_ts, f.forecast_ts) >= CURRENT_DATE - INTERVAL '1' DAY
),
deduplicated AS (
-- Di sini kita selipkan ROW_NUMBER untuk membuang duplikat
SELECT \*,
ROW_NUMBER() OVER (
PARTITION BY city, event_ts
ORDER BY scraped_ts DESC
) as rn
FROM source
)
SELECT
to_hex(md5(to_utf8(s.city|| cast(s.event_ts as varchar)))) as fact_id,
s.event_ts,
c.id as city_id,
coalesce(s.act_aqi,s.fc_aqi) as aqi,
a.id as aqi_id,
coalesce(s.act_weather_condition ,s.fc_weather_condition ) as weather_condition,
COALESCE(s.act_temp_val, s.fc_temp_val) as temp_val,
coalesce(s.act_humdiy_val , s.fc_humidity_val ) as humidity_val,
coalesce(s.act_wind_val , s.fc_wind_val ) as wind_val,
s.pollutant_val,
s.main_pollutant,
s.wind_dir,
s.alert ,
s.scraped_ts as load_ts,
s.observation_ts
FROM deduplicated s
left join iceberg.gold.dim_city c
on s.city = c.city
left join iceberg.gold.dim_aqi a
on coalesce(s.act_aqi,s.fc_aqi) BETWEEN a.min_val AND a.max_val
WHERE s.rn = 1

--and
c.city = 'depok'
ORDER BY event_ts DESC
=======================================================================================
with city_source as (
select
distinct(city),
province,
'Indonesia' as country,
cast(CURRENT_TIMESTAMP as timestamp) as created_ts,
cast(CURRENT_TIMESTAMP as timestamp) as updated_ts
from iceberg.silver.aqi_index
)
select
to_hex(md5(to_utf8(city))) as id,
city,
province,
country,
created_ts,
updated_ts
from city_source

select
to_hex(md5(to_utf8(city))) as id,
city,
province,
'Indonesia' as country
from (
select distinct city, province from iceberg.silver.aqi_index
)

=======================================================================================
MERGE INTO iceberg.gold.dim_aqi as target
using (
select
to_hex(md5(to_utf8(aqi_status))) as id,
aqi_status as category,
MIN(aqi_val) as min_val,
MAX(aqi_val) as max_val,
cast(CURRENT_TIMESTAMP as timestamp) as created_ts,
cast(CURRENT_TIMESTAMP as timestamp) as updated_ts
FROM iceberg.silver.aqi_index
GROUP BY aqi_status
order by min_val
) as source
on target.id = source.id
WHEN MATCHED THEN
UPDATE set
min_val = source.min_val,
max_val = source.max_val,
updated_ts = soource.updated_ts
WHEN NOT MATCHED then
insert (
id,
category,
min_val,
max_val,
created_ts,
updated_ts
)
values (
source.id,
source.category,
source.min_val,
source.max_val,
source.created_ts,
source.updated_ts
)

SELECT
ROW_NUMBER() OVER(ORDER BY min_val) as aqi_range_id,
category,
min_val,
max_val,
CASE
WHEN category = 'Good' THEN '#00E400'
WHEN category = 'Moderate' THEN '#FFFF00'
WHEN category = 'Unhealthy' THEN '#FF0000'
WHEN category = 'Very Unhealthy' THEN '#8F3F97'
ELSE '#CCCCCC' -- Warna default jika kategori baru muncul
END as color_code
FROM aqi_ranges;
=======================================================================================
=======================================================================================
=======================================================================================
=======================================================================================
=======================================================================================
