{{
    config(
        materialized='table',
        tags=['aqi']
    )
}}

with silver as (

    select *
    from {{ ref('aqi') }}

),

dim_city as (

    select *
    from {{ ref('dim_city') }}

),

-- Tentukan rentang waktu: dari observasi paling awal sampai paling akhir
-- yang ada di silver. Spine di-generate cuma sepanjang rentang ini.
hour_bounds as (

    select
        date_trunc('hour', min(observation_ts)) as min_hour,
        date_trunc('hour', max(observation_ts)) as max_hour
    from silver

),

-- Generate SEMUA jam dalam rentang itu, apapun ada datanya atau enggak.
hour_spine as (

    select observation_hour
    from (
        select
            dateadd(hour, seq4(), (select min_hour from hour_bounds)) as observation_hour
        from table(generator(rowcount => 100000))  -- cukup buat ~11 tahun jam
    )
    where observation_hour <= (select max_hour from hour_bounds)

),

-- Cross join: setiap kota WAJIB punya baris di setiap jam pada spine,
-- terlepas dari ada datanya atau tidak.
city_hour_spine as (

    select
        d.city_id,
        d.province,
        d.city,
        hs.observation_hour
    from dim_city d
    cross join hour_spine hs

),

-- Agregasi data yang BENERAN ada (persis logic sebelumnya).
aggregated as (

    select
        d.city_id,
        date_trunc('hour', s.observation_ts) as observation_hour,
        avg(s.aqi_val)                        as avg_aqi,
        min(s.aqi_val)                        as min_aqi,
        max(s.aqi_val)                        as max_aqi,
        avg(s.concentration_val)              as avg_concentration,
        avg(s.temperature_val)                as avg_temperature,
        avg(s.humidity_val)                   as avg_humidity,
        avg(s.wind_speed_val)                 as avg_wind_speed,
        mode(s.aqi_status)                    as dominant_aqi_status,
        count(*)                              as observation_count

    from silver s
    left join dim_city d
        on s.province = d.province
       and s.city = d.city
    group by d.city_id, date_trunc('hour', s.observation_ts)

)

-- Spine (semua jam) LEFT JOIN ke aggregated (data aktual).
-- Jam yang gak ada datanya -> semua metric NULL, observation_count = 0.
select
    md5(concat_ws('|', chs.city_id, to_varchar(chs.observation_hour)))  as hourly_id,
    chs.city_id,
    dt.date_id,
    hr.hour_of_day,
    cat.aqi_id,
    chs.province,
    chs.city,
    chs.observation_hour,
    agg.avg_aqi,
    agg.min_aqi,
    agg.max_aqi,
    agg.avg_concentration,
    agg.avg_temperature,
    agg.avg_humidity,
    agg.avg_wind_speed,
    agg.dominant_aqi_status,
    coalesce(agg.observation_count, 0)                                 as observation_count
from city_hour_spine chs
left join aggregated agg
    on chs.city_id = agg.city_id
   and chs.observation_hour = agg.observation_hour
left join {{ ref('dim_date') }} as dt
    on dt.date_day = to_date(chs.observation_hour)
left join {{ ref('dim_hour') }} as hr
    on hr.hour_of_day = hour(chs.observation_hour)
left join {{ ref('dim_aqi_status') }} as cat
    on cat.aqi_status = agg.dominant_aqi_status