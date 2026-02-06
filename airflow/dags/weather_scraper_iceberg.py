from airflow.sdk import dag, task
from datetime import datetime, timedelta
import pandas as pd
import time
import yaml
import requests
from bs4 import BeautifulSoup
import os
import pendulum
import boto3
from botocore.client import Config
import io
import pyarrow as pa
from pyiceberg.catalog import load_catalog


@dag(
    dag_id="weather_scraper_iceberg",
    schedule="10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "weather", "iceberg"],
)
def forecast_weather():

    @task()
    def extract_forecast_weather():
        # --- 1. CONFIG & SETUP ---
        # Menggunakan AIRFLOW_HOME agar path lebih pasti
        dag_path = os.path.dirname(os.path.realpath(__file__))
        cfg_file = os.path.join(dag_path, "config.yaml")

        if not os.path.exists(cfg_file):
            print(f"Error: Config file not found at {cfg_file}")
            return []

        with open(cfg_file, "r") as file:
            scraping_cfg = yaml.safe_load(file)

        locations = scraping_cfg.get("locations", [])
        weather_map = scraping_cfg.get("weather_map", {})
        local_tz = pendulum.timezone("Asia/Jakarta")
        all_forecasts = []

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36..."
        }

        # --- 2. SCRAPING PROCESS ---
        for loc in locations:
            province = loc["province"]
            for city in loc["cities"]:
                url = f"https://www.iqair.com/indonesia/{province}/{city}"
                try:
                    print(f"[*] Scraping Current Data: {city}...")
                    response = requests.get(url, headers=headers, timeout=15)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "html.parser")

                    forecast_table = soup.find("table")
                    if forecast_table:
                        forecast_items = forecast_table.find_all("td")
                        tracking_date = datetime.now(local_tz)
                        last_hour = -1
                        ingest_ts = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")

                        for item in forecast_items:
                            try:
                                time_element = item.find("p", class_="max-w-12")
                                if not time_element:
                                    continue

                                time_str = time_element.get_text(strip=True)
                                current_hour = (
                                    datetime.now(local_tz).hour
                                    if time_str.lower() == "now"
                                    else int(time_str.split(":")[0])
                                )

                                if current_hour < last_hour:
                                    tracking_date += timedelta(days=1)
                                last_hour = current_hour

                                full_ts = f"{tracking_date.strftime('%Y-%m-%d')} {current_hour:02d}:00:00"

                                # -- Data extraction --
                                weather_desc = "Unknown"
                                icon_tag = item.find("img")
                                icon_src = icon_tag.get("src") if icon_tag else ""
                                if icon_src:
                                    try:
                                        file_name = icon_src.split("/")[-1]
                                        code_key = file_name.split("-")[2][:2]
                                        weather_desc = weather_map.get(
                                            code_key, f"Code {code_key}"
                                        )
                                    except Exception as e:
                                        weather_desc = "Error Parsing Code"

                                aqi_div = item.find(
                                    "div", class_=lambda x: x and "aqi-bg-" in x
                                )
                                aqi = aqi_div.get_text(strip=True) if aqi_div else "N/A"

                                temp = next(
                                    (
                                        p.text
                                        for p in item.find_all(
                                            "p", class_="font-medium"
                                        )
                                        if "°" in p.text
                                    ),
                                    "N/A",
                                )

                                metrics = item.find_all(
                                    "div", class_="flex flex-col items-center"
                                )
                                wind = (
                                    metrics[0].find("p", class_="font-medium").text
                                    if len(metrics) > 0
                                    else "-"
                                )
                                hum = (
                                    metrics[1].find("p", class_="font-medium").text
                                    if len(metrics) > 1
                                    else "-"
                                )

                                # web data time
                                h2_element = soup.find("h2")
                                if h2_element and "•" in h2_element.get_text():
                                    try:
                                        observation_time = (
                                            h2_element.get_text(strip=True)
                                            .split("•")[-1]
                                            .replace("Local time", "")
                                            .strip()
                                        )
                                    except Exception as e:
                                        print(
                                            f"DEBUG: Failed to parse time_info for {city}: {e}"
                                        )

                                all_forecasts.append(
                                    {
                                        "city": city.replace("-", " ").title(),
                                        "forecast_ts": full_ts,
                                        "aqi": aqi,
                                        "weather": weather_desc,
                                        "temperature": temp,
                                        "wind_speed": wind,
                                        "humidity": hum,
                                        "observation_time": observation_time,
                                        "scraped_at": ingest_ts,
                                    }
                                )
                            except Exception as e:
                                print(f"Error parsing item for {city}: {e}")
                except Exception as e:
                    print(f"Error connecting to {city}: {e}")

                time.sleep(5)

        if not all_forecasts:
            return "No Data Scraped"

        df = pd.DataFrame(all_forecasts)
        arrow_table = pa.Table.from_pandas(df)

        catalog = load_catalog(
            "default",
            **{
                "type": "hive",
                "uri": "thrift://hive-metastore:9083",
                "s3.endpoint": "http://minio:9000",
                "s3.access-key-id": "minioadmin",
                "s3.secret-access-key": "minioadmin",
                "s3.use-ssl": "false",
                "s3.region-name": "us-east-1",
            },
        )

        tgt_namespace = "bronze"
        try:
            catalog.create_namespace(tgt_namespace)
        except Exception:
            pass

        table_name = "raw_weather_forecast"
        table_id = f"{tgt_namespace}.{table_name}"

        try:
            table = catalog.load_table(table_id)
            print(f"ℹ️ Table {table_id} found. Appending data...")
        except Exception:
            print(f"✨ Creating new table {table_id}...")
            table = catalog.create_table(
                identifier=table_id,
                schema=arrow_table.schema,
                location=f"s3a://lakehouse/{tgt_namespace}/{table_name}",
            )
        table.append(arrow_table)
        return f"🚀 Done! {len(all_forecasts)} rows added to {table_id}"

    # Memanggil task di dalam DAG
    extract_forecast_weather()


# Inisialisasi DAG
dag_instance = forecast_weather()
