from airflow.sdk import dag, task
from trino.dbapi import connect
from datetime import datetime
from airflow.datasets import Dataset

# --- KONFIGURASI HARDCODE ---
TRINO_CONFIG = {
    "host": "trino",
    "port": 8080,
    "user": "admin",
    "catalog": "iceberg",
}

AQI_BRONZE = Dataset("s3a://lakehouse/bronze/raw_aqi_index")
FORECAST_BRONZE = Dataset("s3a://lakehouse/bronze/raw_weather_forecast")

AQI_SILVER = Dataset("s3a://lakehouse/silver/aqi_index")
FORECAST_SILVER = Dataset("s3a://lakehouse/silver/weather_forecast")


@dag(
    dag_id="02_silver_load_aqi_weather",
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
    def table_preparation_aqi_index():
        query = """
        CREATE TABLE IF NOT EXISTS iceberg.silver.aqi_index (
            id VARCHAR,
            province VARCHAR,
            city VARCHAR,
            event_ts TIMESTAMP(6),
            aqi_val INTEGER,
            aqi_status VARCHAR,
            main_pollutant VARCHAR,
            pollutant_val DOUBLE,
            weather_condition VARCHAR,
            temp_val DOUBLE,
            humidity_val INTEGER,
            wind_val DOUBLE,
            wind_dir VARCHAR,
            alert VARCHAR,
            scraped_ts TIMESTAMP(6)
        ) 
        WITH (
            partitioning = ARRAY['city', 'month(event_ts)'],
            location = 's3a://lakehouse/silver/aqi_index/',
            format = 'PARQUET'
        )
        """
        execute_trino(query)
        print("Table 'aqi_index' prepared.")

    @task()
    def table_preparation_forecast_weather():
        query = """
        CREATE TABLE IF NOT EXISTS iceberg.silver.forecast_weather (
            id VARCHAR,
            province VARCHAR,
            city VARCHAR,
            forecast_ts TIMESTAMP(6),
            observation_ts TIMESTAMP(6),
            aqi_val INTEGER,
            weather_condition VARCHAR,
            temp_val DOUBLE,
            wind_val DOUBLE,
            humidity_val INTEGER,
            scraped_ts TIMESTAMP(6)
        )
        WITH (
            partitioning = ARRAY['city', 'month(observation_ts)'],
            location = 's3a://lakehouse/silver/forecast_weather/',
            format = 'PARQUET'
        )
        """
        execute_trino(query)
        print("Table 'forecast_weather' prepared.")

    @task(outlets=[AQI_SILVER])
    def upsert_aqi_index():
        query = """
        MERGE INTO iceberg.silver.aqi_index AS target
        USING (
            WITH cleaned_source AS (
                SELECT 
                    LOWER(TRIM(province)) as province ,
                    LOWER(TRIM(city)) as city,
                    CAST(
                        parse_datetime(
                            observation_time || ', ' || CAST(year(CAST(scraped_at AS TIMESTAMP)) AS VARCHAR), 
                            'HH:mm, MMM dd, yyyy'
                        ) AS TIMESTAMP
                    ) as event_ts,                    
                    CAST(regexp_replace(aqi, '[^\d]', '') AS INTEGER) as aqi_val,
                    aqi_status,
                    main_pollutant,                    
                    CAST(regexp_replace(concentration, '[^\d.-]', '') AS DOUBLE) as pollutant_val,
                    weather as weather_condition,
                    CAST(regexp_replace(temperature, '[^\d.-]', '') AS DOUBLE) as temp_val,
                    CAST(regexp_replace(humidity, '[^\d.-]', '') AS INTEGER) as humidity_val,
                    CAST(regexp_replace(wind_speed, '[^\d.-]', '') AS DOUBLE) as wind_val,        
                    wind_direction as wind_dir,
                    alert as alert,
                    CAST(scraped_at AS TIMESTAMP) as scraped_ts
                FROM iceberg.bronze.raw_aqi_index
            ),
            deduplicated AS (
                SELECT *,
                    to_hex(md5(to_utf8(city || CAST(event_ts AS VARCHAR)))) as id,
                	ROW_NUMBER() OVER (PARTITION BY city, event_ts ORDER BY scraped_ts DESC) as rn
                FROM cleaned_source
            )
            SELECT * FROM deduplicated WHERE rn = 1
        ) AS source
        ON target.id = source.id

        WHEN MATCHED THEN
            UPDATE SET
                aqi_val = source.aqi_val,
                aqi_status = source.aqi_status,
                main_pollutant = source.main_pollutant,
                pollutant_val = source.pollutant_val,
                weather_condition = source.weather_condition,
                temp_val = source.temp_val,
                humidity_val = source.humidity_val,
                wind_val = source.wind_val,
                wind_dir = source.wind_dir,
                alert = source.alert

        WHEN NOT MATCHED THEN
            INSERT (
                id, 
                province, 
                city, 
                event_ts, 
                aqi_val, 
                aqi_status, 
                main_pollutant, 
                pollutant_val, 
                weather_condition, 
                temp_val, 
                humidity_val, 
                wind_val, 
                wind_dir, 
                alert, 
                scraped_ts
            )
            VALUES (
                source.id, 
                source.province, 
                source.city, 
                source.event_ts, 
                source.aqi_val, 
                source.aqi_status, 
                source.main_pollutant, 
                source.pollutant_val, 
                source.weather_condition, 
                source.temp_val, 
                source.humidity_val, 
                source.wind_val, 
                source.wind_dir, 
                source.alert, 
                source.scraped_ts
            )
        """
        execute_trino(query)
        print("Data successfully merged into silver.aqi_index")

    @task(outlets=[FORECAST_SILVER])
    def upsert_forecast_weather():
        query = """
        MERGE INTO iceberg.silver.forecast_weather AS target
        USING (
            WITH cleaned_source AS (
                SELECT 
                    LOWER(TRIM(province)) as province,
                    LOWER(TRIM(city)) as city,
        			CAST(forecast_ts AS TIMESTAMP) as forecast_ts,
        			CAST(parse_datetime(
                        format_datetime(CAST(scraped_at AS TIMESTAMP), 'yyyy-MM-dd') || ' ' || 
                        split_part(observation_time, ',', 1), 
                        'yyyy-MM-dd HH:mm'
                    ) AS TIMESTAMP) as observation_ts,
                    CAST(aqi AS INTEGER) as aqi_val,
                    weather as weather_condition,
                    CAST(regexp_replace(temperature, '[^\d.-]', '') AS DOUBLE) as temp_val,
                    CAST(regexp_replace(wind_speed, '[^\d.-]', '') AS DOUBLE) as wind_val,
                    CAST(regexp_replace(humidity, '[^\d.-]', '') AS INTEGER) as humidity_val,
                    CAST(scraped_at AS TIMESTAMP) as scraped_ts
                FROM iceberg.bronze.raw_weather_forecast
            ),
            deduplicated AS (
                SELECT *,
                    to_hex(md5(to_utf8(city || CAST(forecast_ts AS VARCHAR)))) as id,
                    ROW_NUMBER() OVER (PARTITION BY city, forecast_ts ORDER BY scraped_ts DESC) as rn
                FROM cleaned_source
            )
            SELECT * FROM deduplicated 
            WHERE rn = 1 
        ) AS source
        ON target.id = source.id

        WHEN MATCHED THEN
            UPDATE SET
                forecast_ts = source.forecast_ts,
                observation_ts = source.observation_ts,
                aqi_val = source.aqi_val,
                weather_condition = source.weather_condition,
                temp_val = source.temp_val,
                wind_val = source.wind_val,
                humidity_val = source.humidity_val,
                scraped_ts = source.scraped_ts

        WHEN NOT MATCHED THEN
            INSERT (
                id,
                province,
                city,
                forecast_ts,
                observation_ts,
                aqi_val,
                weather_condition,
                temp_val,
                wind_val,
                humidity_val,
                scraped_ts
            )
            VALUES (
                source.id,
                source.province,
                source.city,
                source.forecast_ts,
                source.observation_ts,
                source.aqi_val,
                source.weather_condition,
                source.temp_val,
                source.wind_val,
                source.humidity_val,
                source.scraped_ts
            )
        """
        execute_trino(query)
        print("Data successfully merged into silver.forecast_weather")

    # Flow DAG

    start_preps = schema_preparation()
    start_preps >> table_preparation_aqi_index() >> upsert_aqi_index()
    start_preps >> table_preparation_forecast_weather() >> upsert_forecast_weather()


# Inisialisasi DAG
weather_aqi_dag = weather_aqi_pipeline()
