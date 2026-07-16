{{
    config(
        materialized='incremental',
        unique_key='aqi_id',
        incremental_strategy='merge',
        merge_exclude_columns=['insert_ts'],
        tags=['aqi']
    )
}}

with source_aqi_status as (

    select 
        aqi_status,
        min(aqi_val) as min_val,
        max(aqi_val) as max_val
    from {{ ref('aqi') }}
    group by aqi_status

),

new_aqi_status as (

    select *
    from source_aqi_status as sas

    {% if is_incremental() %}
    -- Menggunakan pola yang sama dengan dim_city:
    -- Hanya ambil data jika kombinasi ID + MIN + MAX BELUM ada di target.
    -- Jika ada salah satu nilai (min_val / max_val) yang berubah, 
    -- kueri ini akan menganggapnya sebagai record baru untuk di-MERGE (UPDATE).
    where not exists (
        select 1
        from {{ this }} as t
        where t.aqi_id = md5(concat_ws('|', sas.aqi_status))
        --   and t.min_val = sas.min_val
        --   and t.max_val = sas.max_val
    )
    {% endif %}

)

select
    md5(concat_ws('|', aqi_status)) as aqi_id,
    aqi_status,
    min_val,
    max_val,
    current_timestamp() as insert_ts,
    current_timestamp() as update_ts
from new_aqi_status