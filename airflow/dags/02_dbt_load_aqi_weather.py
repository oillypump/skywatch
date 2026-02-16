from airflow.sdk import dag, task
from trino.dbapi import connect
from datetime import datetime
from airflow.datasets import Dataset
from airflow.providers.standard.operators.bash import BashOperator


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
    dag_id="02_dbt_load_aqi_weather",
    start_date=datetime(2026, 2, 1),
    schedule=(AQI_BRONZE, FORECAST_BRONZE),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "iceberg"],
)
def weather_aqi_pipeline():
    """
    LAYER : SILVER
    --- deduplication
    --- cleansing
    """

    dbt_aqi_index = BashOperator(
        task_id="dbt_aqi_index",
        bash_command="""
        cd /opt/airflow/dbt/skywatch_transform && \
        dbt run --select aqi_index --profiles-dir ..
        """,
        outlets=[AQI_SILVER],
    )

    dbt_forecast_weather = BashOperator(
        task_id="dbt_forecast_weather",
        bash_command="""
        cd /opt/airflow/dbt/skywatch_transform && \
        dbt run --select weather_forecast --profiles-dir ..
        """,
        outlets=[FORECAST_SILVER],
    )
    # Flow DAG
    [dbt_aqi_index, dbt_forecast_weather]


# Inisialisasi DAG
weather_aqi_dag = weather_aqi_pipeline()
