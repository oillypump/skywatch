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
}


@dag(
    dag_id="03_gold_load_aqi_weather",
    start_date=datetime(2026, 2, 1),
    schedule=None,
    catchup=False,
    tags=["gold", "iceberg"],
)
def weather_aqi_pipeline():
    """
    LAYER : GOLD
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
            CREATE SCHEMA IF NOT EXISTS gold.silver
            WITH (location = 's3a://lakehouse/gold/')
        """
        execute_trino(query)
        print("Schema 'gold' prepared.")

    @task()
    def table_preparation():
        queries = [
            # 1. FACT TABLE (Sudah ditambahkan Foreign Keys agar bisa JOIN)
            """
            CREATE TABLE IF NOT EXISTS iceberg.gold.fact_aqi_weather_hourly (
                event_ts TIMESTAMP(6),
                date_key DATE,           -- Join ke dim_date
                location_key VARCHAR,    -- Join ke dim_city
                aqi_category_key VARCHAR,-- Join ke dim_aqi_category
                pollutant_key VARCHAR,   -- Join ke dim_pollutant
                wind_key VARCHAR,        -- Join ke dim_wind_direction
                source_key VARCHAR,      -- Join ke dim_source_type
                weather_key VARCHAR,     -- Join ke dim_weather
                alert_key VARCHAR,       -- Join ke dim_alert
                city VARCHAR,            -- Denormalisasi untuk filter cepat
                aqi INTEGER,
                pollutant_value DOUBLE,  -- Nilai angka murni (misal 16.9)
                temp_c DOUBLE,
                humidity INTEGER,
                data_type VARCHAR
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['month(event_ts)'],
                location = 's3a://lakehouse/gold/fact_aqi_weather_hourly/'
            )
            """,
            "ALTER TABLE iceberg.gold.fact_aqi_weather_hourly SET PROPERTIES sorting_columns = ARRAY['city', 'event_ts']",
            # 2. DIMENSIONS (Perbaikan quotes dan struktur)
            "CREATE TABLE IF NOT EXISTS iceberg.gold.dim_city (location_key VARCHAR, city VARCHAR, province VARCHAR, country VARCHAR) WITH (format = 'PARQUET', location = 's3a://lakehouse/gold/dim_city/')",
            "CREATE TABLE IF NOT EXISTS iceberg.gold.dim_date (date_key DATE, day_of_week INTEGER, day_name VARCHAR, month_name VARCHAR, quarter INTEGER, year INTEGER, is_weekend BOOLEAN) WITH (format = 'PARQUET', location = 's3a://lakehouse/gold/dim_date/')",
            "CREATE TABLE IF NOT EXISTS iceberg.gold.dim_weather (weather_key VARCHAR, condition VARCHAR, is_rainy BOOLEAN, severity_level INTEGER) WITH (format = 'PARQUET', location = 's3a://lakehouse/gold/dim_weather/')",
            "CREATE TABLE IF NOT EXISTS iceberg.gold.dim_alert (alert_key VARCHAR, alert_name VARCHAR, severity_level VARCHAR, color_code VARCHAR) WITH (format = 'PARQUET', location = 's3a://lakehouse/gold/dim_alert/')",
            "CREATE TABLE IF NOT EXISTS iceberg.gold.dim_pollutant (pollutant_key VARCHAR, pollutant_name VARCHAR, unit VARCHAR, description VARCHAR) WITH (format = 'PARQUET', location = 's3a://lakehouse/gold/dim_pollutant/')",
            "CREATE TABLE IF NOT EXISTS iceberg.gold.dim_wind_direction (wind_key VARCHAR, direction VARCHAR, description VARCHAR) WITH (format = 'PARQUET', location = 's3a://lakehouse/gold/dim_wind_direction/')",
            "CREATE TABLE IF NOT EXISTS iceberg.gold.dim_source_type (source_key VARCHAR, source_name VARCHAR, reliability_level INTEGER) WITH (format = 'PARQUET', location = 's3a://lakehouse/gold/dim_source_type/')",
            "CREATE TABLE IF NOT EXISTS iceberg.gold.dim_aqi_category (aqi_category_key VARCHAR, category_name VARCHAR, aqi_min INTEGER, aqi_max INTEGER, health_recommendation VARCHAR) WITH (format = 'PARQUET', location = 's3a://lakehouse/gold/dim_aqi_category/')",
        ]

        for q in queries:
            execute_trino(q)
        print("Star Schema Gold Layer Prepared with all Dimensions.")

    @task()
    def upsert_bronze_to_silver():
        query = """
 

            """
        execute_trino(query)
        print("Data upserted successfully.")

    # Flow DAG
    schema_preparation() >> table_preparation() >> upsert_bronze_to_silver()


# Inisialisasi DAG
weather_aqi_dag = weather_aqi_pipeline()
