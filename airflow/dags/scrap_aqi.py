from datetime import datetime

import pendulum
from airflow.sdk import dag, task

from scrapers import aqi_scraper


@dag(
    dag_id="scrap_aqi_weather",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "snowflake", "example"],
)
def air_quality_and_forecast_weather():
    @task(retries=2, retry_delay=pendulum.duration(minutes=2))
    def create_table():
        aqi_scraper.create_table()

    @task(retries=2, retry_delay=pendulum.duration(minutes=2))
    def extract_aqi():
        aqi_scraper.run()

    create_table() >> extract_aqi()


dag_obj = air_quality_and_forecast_weather()
