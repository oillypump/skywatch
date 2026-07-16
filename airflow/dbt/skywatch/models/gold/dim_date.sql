{{
    config(
        materialized='incremental',
        unique_key='date_id',
        incremental_strategy='merge',
        merge_exclude_columns=['insert_ts'],
        tags=['aqi']
    )
}}

with date_bounds as (

    select
        min(observation_ts)::date as min_date,
        max(observation_ts)::date as max_date
    from {{ ref('aqi') }}

),

-- Generate SEMUA tanggal di rentang min-max, konsisten dengan pendekatan
-- hour_spine di fct_aqi_hourly (biar date_id gak pernah NULL walau ada
-- hari yang sepenuhnya tanpa data).
--
-- CATATAN: Snowflake GENERATOR(rowcount => ...) WAJIB literal constant,
-- gak bisa pakai subquery (walau subquery-nya cuma return 1 angka) --
-- itu yang bikin error "argument 1 to function GENERATOR needs to be
-- constant". Makanya rowcount di-hardcode sebagai UPPER BOUND aman,
-- lalu di-filter WHERE supaya hasil akhirnya tetap PERSIS sesuai
-- rentang min-max (bukan berarti selalu generate 3650 hari beneran).
date_spine as (

    select date_day
    from (
        select
            dateadd(day, seq4(), (select min_date from date_bounds)) as date_day
        from table(generator(rowcount => 3650))  -- upper bound aman (~10 tahun)
    )
    where date_day <= (select max_date from date_bounds)

),

new_dates as (

    select *
    from date_spine as ds

    {% if is_incremental() %}
    -- Anti-join: cuma ambil tanggal yang belum ada di dim_date.
    -- Karena max_date terus maju seiring data baru masuk, tanggal baru
    -- otomatis ke-generate & ke-insert di run berikutnya.
    where not exists (
        select 1
        from {{ this }} as t
        where t.date_id = to_char(ds.date_day, 'YYYYMMDD')::number
    )
    {% endif %}

)

select
    to_char(date_day, 'YYYYMMDD')::number  as date_id,
    date_day,
    year(date_day)                          as year,
    quarter(date_day)                       as quarter,
    month(date_day)                         as month,
    monthname(date_day)                     as month_name,
    day(date_day)                           as day_of_month,
    dayofweek(date_day)                     as day_of_week,   -- 0=Minggu ... 6=Sabtu
    dayname(date_day)                       as day_name,
    case
        when dayofweek(date_day) in (0, 6) then true
        else false
    end                                      as is_weekend,
    current_timestamp()  as insert_ts,
    current_timestamp()  as update_ts
from new_dates