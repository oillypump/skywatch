{{
    config(
        materialized='incremental',
        unique_key='city_id',
        incremental_strategy='merge',
        merge_exclude_columns=['insert_ts'],
        tags=['aqi']
    )
}}

with source_cities as (

    select distinct
        province,
        city
    from {{ ref('aqi') }}

),

new_cities as (

    select *
    from source_cities as sc

    {% if is_incremental() %}
    -- Cuma ambil kombinasi province+city yang BELUM ada di dim_city.
    -- Anti-join ini yang bikin ini "upsert" (insert kota baru saja),
    -- bukan compare timestamp seperti model incremental lain, karena
    -- dim_city gak punya kolom waktu sendiri untuk dijadiin watermark.
    where not exists (
        select 1
        from {{ this }} as t
        where t.city_id = md5(concat_ws('|', sc.province, sc.city))
    )
    {% endif %}

)

select
    md5(concat_ws('|', province, city))  as city_id,
    province,
    city,
    current_timestamp()  as insert_ts,
    current_timestamp()  as update_ts
    -- Kolom atribut tambahan nanti bisa ditambahkan di sini,
    -- misal: latitude, longitude, population, region_group, dll.
from new_cities