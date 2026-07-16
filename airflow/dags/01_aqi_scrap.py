from datetime import datetime

import pendulum
from airflow.sdk import dag, task, task_group
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator

import aqi_scraper

# ---- Single source of truth untuk semua config koneksi ----
SNOWFLAKE_CONN_ID = "snowflake_conn"
DATABASE = "WEATHER"
SCHEMA = "BRONZE"
TABLE = "AQI_RAW"


@dag(
    dag_id="01_aqi_scrap",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "snowflake", "example"],
)
def aqi_scrap():
    @task_group(group_id="setup")
    def setup():
        create_database = SQLExecuteQueryOperator(
            task_id="create_database",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=f"CREATE DATABASE IF NOT EXISTS {DATABASE};",
            retries=2,
            retry_delay=pendulum.duration(minutes=2),
        )

        create_schema = SQLExecuteQueryOperator(
            task_id="create_schema",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{SCHEMA};",
            retries=2,
            retry_delay=pendulum.duration(minutes=2),
        )

        create_table = SQLExecuteQueryOperator(
            task_id="create_table",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=aqi_scraper.build_create_table_sql(
                database=DATABASE,
                schema=SCHEMA,
                table=TABLE,
            ),
            retries=2,
            retry_delay=pendulum.duration(minutes=2),
        )

        create_database >> create_schema >> create_table

    @task(retries=2, retry_delay=pendulum.duration(minutes=2))
    def extract_aqi():
        aqi_scraper.run(
            conn_id=SNOWFLAKE_CONN_ID,
            database=DATABASE,
            schema=SCHEMA,
            table=TABLE,
        )

    setup() >> extract_aqi()


dag_obj = aqi_scrap()
