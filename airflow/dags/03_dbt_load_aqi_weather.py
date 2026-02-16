from airflow.sdk import dag, task
from trino.dbapi import connect
from datetime import datetime
from airflow.datasets import Dataset
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models.baseoperator import cross_downstream


AQI_SILVER = Dataset("s3a://lakehouse/silver/aqi_index")
FORECAST_SILVER = Dataset("s3a://lakehouse/silver/weather_forecast")


@dag(
    dag_id="03_dbt_load_aqi_weather",
    start_date=datetime(2026, 2, 1),
    schedule=(AQI_SILVER, FORECAST_SILVER),
    catchup=False,
    tags=["gold", "iceberg"],
)
def weather_aqi_pipeline():
    """
    LAYER : GOLD
    """
    run_snapshot_city = BashOperator(
        task_id="run_snapshot_city",
        bash_command="""
        cd /opt/airflow/dbt/skywatch_transform && \
        dbt snapshot --select scd_city --profiles-dir ..
        """,
    )

    run_snapshot_aqi = BashOperator(
        task_id="run_snapshot_aqi",
        bash_command="""
        cd /opt/airflow/dbt/skywatch_transform && \
        dbt snapshot --select scd_aqi --profiles-dir ..
        """,
    )

    dbt_dim_city = BashOperator(
        task_id="dbt_dim_city",
        bash_command="""
        cd /opt/airflow/dbt/skywatch_transform && \
        dbt run --select dim_city --profiles-dir ..
        """,
    )

    dbt_dim_aqi = BashOperator(
        task_id="dbt_dim_aqi",
        bash_command="""
        cd /opt/airflow/dbt/skywatch_transform && \
        dbt run --select dim_aqi --profiles-dir ..
        """,
    )

    dbt_fact_aqi_weather = BashOperator(
        task_id="dbt_fact_aqi_weather",
        bash_command="""
        cd /opt/airflow/dbt/skywatch_transform && \
        dbt run --select fact_aqi_weather --profiles-dir ..
        """,
    )

    # Flow DAG

    (
        run_snapshot_city
        >> run_snapshot_aqi
        >> [dbt_dim_city, dbt_dim_aqi]
        >> dbt_fact_aqi_weather
    )


# Inisialisasi DAG
weather_aqi_dag = weather_aqi_pipeline()
