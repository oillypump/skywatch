from airflow.decorators import dag, task
from trino.dbapi import connect
from datetime import datetime
from airflow.datasets import Dataset

# --- KONFIGURASI HARDCODE ---
TRINO_CONFIG = {
    "host": "trino",
    "port": 8080,
    "user": "admin",
    "catalog": "iceberg",
    # "schema": "silver",
    # "auth": BasicAuthentication("admin", "password_kamu") # Uncomment jika butuh auth
}

AQI_BRONZE = Dataset("s3a://lakehouse/bronze/raw_aqi_index")
FORECAST_BRONZE = Dataset("s3a://lakehouse/bronze/raw_weather_forecast")


@dag(
    dag_id="silver_load_aqi_weather",
    start_date=datetime(2026, 2, 1),
    schedule=(AQI_BRONZE, FORECAST_BRONZE),
    catchup=False,
    tags=["silver", "iceberg"],
)
def weather_aqi_pipeline():
    """
    LAYER : SILVER
    --- deduplication
    --- cleansing
    """

    def execute_trino(sql):
        """Helper untuk koneksi langsung ke Trino"""
        conn = connect(**TRINO_CONFIG)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            # return fetchall hanya jika ada data, kalau DDL/DML biasanya return list kosong
            try:
                return cur.fetchall()
            except:
                return None
        finally:
            conn.close()

    @task()
    def schema_preparation():
        query = """
            CREATE SCHEMA IF NOT EXISTS iceberg.silver
            WITH (location = 's3a://lakehouse/silver/')
        """
        execute_trino(query)
        print("Schema 'silver' prepared.")

    @task()
    def table_preparation():
        query = """
        CREATE TABLE IF NOT EXISTS iceberg.silver.weather_aqi_hourly (
            silver_record_id VARCHAR,
            city VARCHAR,
            event_ts TIMESTAMP(6),
            aqi INTEGER,
            aqi_status VARCHAR,
            main_pollutant VARCHAR,
            concentration VARCHAR,
            weather VARCHAR,
            temp_c DOUBLE,
            humidity_pct INTEGER,
            wind_speed_kmh DOUBLE,
            wind_direction VARCHAR,
            alert VARCHAR,
            scraped_ts TIMESTAMP(6),
            data_source VARCHAR

        )
        WITH (
            partitioning = ARRAY['city', 'month(event_ts)'],
            location = 's3a://lakehouse/silver/weather_aqi_hourly',
            format = 'PARQUET'
        )
        """
        execute_trino(query)
        print("Table 'weather_aqi_hourly' prepared.")

    @task()
    def upsert_bronze_to_silver():
        query = """
        MERGE INTO iceberg.silver.weather_aqi_hourly AS target
            USING (
                WITH unified_data AS (
                    -- AQI: Data Aktual
                    SELECT 
                        to_hex(md5(to_utf8(LOWER(TRIM(city)) || CAST(observation_time AS VARCHAR)))) as record_id,
                        LOWER(TRIM(city)) as city,
                        CAST(aqi AS INTEGER) as aqi,
                        aqi_status,
                        main_pollutant,
                        concentration,
                        weather,
                        CAST(regexp_replace(temperature, '[^\d.-]', '') AS DOUBLE) as temp_c,
                        CAST(regexp_replace(humidity, '[^\d.-]', '') AS INTEGER) as humidity_pct,
                        CAST(regexp_replace(wind_speed, '[^\d.-]', '') AS DOUBLE) as wind_speed_kmh,
                        wind_direction,
                        alert,
                        CAST(parse_datetime(observation_time || ', 2026', 'HH:mm, MMM dd, yyyy') AS TIMESTAMP) as event_ts,
                        CAST(scraped_at AS TIMESTAMP) as scraped_ts,
                        'aqi_index' as data_source
                    FROM iceberg.bronze.raw_aqi_index

                    UNION ALL

                    -- FORECAST: Data Ramalan
                    SELECT 
                        to_hex(md5(to_utf8(LOWER(TRIM(city)) || CAST(forecast_ts AS VARCHAR)))) as record_id,
                        LOWER(TRIM(city)) as city,
                        CAST(aqi AS INTEGER) as aqi,
                        CAST(NULL AS VARCHAR) as aqi_status,
                        CAST(NULL AS VARCHAR) as main_pollutant,
                        CAST(NULL AS VARCHAR) as concentration,
                        weather,
                        CAST(regexp_replace(temperature, '[^\d.-]', '') AS DOUBLE) as temp_c,
                        CAST(regexp_replace(humidity, '[^\d.-]', '') AS INTEGER) as humidity_pct,
                        CAST(regexp_replace(wind_speed, '[^\d.-]', '') AS DOUBLE) as wind_speed_kmh,
                        CAST(NULL AS VARCHAR) as wind_direction,
                        CAST(NULL AS VARCHAR) as alert,
                        CAST(forecast_ts as TIMESTAMP) as event_ts,
                        CAST(scraped_at AS TIMESTAMP) as scraped_ts,
                        'forecast_weather' as data_source
                    FROM iceberg.bronze.raw_weather_forecast
                ),
                final_deduplication AS (
                    SELECT 
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY city, event_ts 
                            ORDER BY 
                                CASE WHEN data_source = 'aqi_index' THEN 1 ELSE 2 END ASC,
                                scraped_ts DESC
                        ) as rn
                    FROM unified_data
                )
                SELECT 
                    to_hex(md5(to_utf8(city || CAST(event_ts AS VARCHAR)))) as silver_record_id,
                    city,
                    event_ts,
                    aqi,
                    aqi_status,
                    main_pollutant,
                    concentration,
                    weather,
                    temp_c,
                    humidity_pct,
                    wind_speed_kmh,
                    wind_direction,
                    alert,
                    scraped_ts,
                    data_source
                FROM final_deduplication 
                WHERE rn = 1
            ) AS source
            ON target.silver_record_id = source.silver_record_id

            WHEN MATCHED THEN
                UPDATE SET
                    aqi = source.aqi,
                    aqi_status = source.aqi_status,
                    main_pollutant = source.main_pollutant,
                    concentration =source.concentration,
                    weather = source.weather,
                    temp_c = source.temp_c,
                    humidity_pct = source.humidity_pct,
                    wind_speed_kmh = source.wind_speed_kmh,
                    wind_direction =source.wind_direction,
                    alert = source.alert,
                    scraped_ts = source.scraped_ts,
                    data_source = source.data_source

            WHEN NOT MATCHED THEN
                INSERT (
                    silver_record_id, city, event_ts, aqi, aqi_status, 
                    main_pollutant, concentration, weather, temp_c, 
                    humidity_pct, wind_speed_kmh, wind_direction, 
                    alert, scraped_ts, data_source
                )
                VALUES (
                    source.silver_record_id, source.city, source.event_ts, source.aqi, source.aqi_status, 
                    source.main_pollutant, source.concentration, source.weather, source.temp_c, 
                    source.humidity_pct, source.wind_speed_kmh, source.wind_direction, 
                    source.alert, source.scraped_ts, source.data_source
                )

            """
        execute_trino(query)
        print("Data upserted successfully.")

    # Flow DAG
    schema_preparation() >> table_preparation() >> upsert_bronze_to_silver()


# Inisialisasi DAG
weather_aqi_dag = weather_aqi_pipeline()
