from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
import pyarrow as pa
import time
import yaml
import requests
from bs4 import BeautifulSoup
from datetime import timedelta
import os
import io
import boto3
from botocore.client import Config
import pendulum
from pyiceberg.catalog import load_catalog
import re
from airflow.datasets import Dataset

AQI_BRONZE = Dataset("s3a://lakehouse/bronze/raw_aqi_index")
FORECAST_BRONZE = Dataset("s3a://lakehouse/bronze/raw_weather_forecast")


@dag(
    dag_id="01_bronze_load_aqi_weather",
    schedule="*/10 * * * *",
    # schedule="5,35 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "iceberg"],
)
def air_quality_and_forecast_weather():
    @task(outlets=[AQI_BRONZE])
    def extract_aqi():
        """
        LAYER : BRONZE
        extract_data
        """

        dag_path = os.path.dirname(os.path.realpath(__file__))
        cfg_file = os.path.join(dag_path, "config.yaml")

        if not os.path.exists(cfg_file):
            raise FileNotFoundError(f"File config.yaml coould not found {cfg_file}")

        with open(cfg_file, "r") as file:
            scraping_cfg = yaml.safe_load(file)

        weather_map = scraping_cfg.get("weather_map", {})
        locations = scraping_cfg.get("locations", [])

        def get_cardinal(style_str):
            try:
                angle = int(re.search(r"(\d+)", style_str).group(1))

                angle = (angle + 180) % 360

                if (angle >= 337.5) or (angle < 22.5):
                    return "North"
                if (angle >= 22.5) and (angle < 67.5):
                    return "North East"
                if (angle >= 67.5) and (angle < 112.5):
                    return "East"
                if (angle >= 112.5) and (angle < 157.5):
                    return "South East"
                if (angle >= 157.5) and (angle < 202.5):
                    return "South"
                if (angle >= 202.5) and (angle < 247.5):
                    return "South West"
                if (angle >= 247.5) and (angle < 292.5):
                    return "West"
                if (angle >= 292.5) and (angle < 337.5):
                    return "North West"
            except:
                return "N/A"

        local_tz = pendulum.timezone("Asia/Jakarta")

        current_data = []

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }

        for loc in locations:
            province = loc["province"]
            for city in loc["cities"]:
                url = f"https://www.iqair.com/indonesia/{province}/{city}"

                # Inisialisasi variabel (Default N/A)
                aqi_val = aqi_stat = pollutant = conc = temp = wind = hum = (
                    wind_direction
                ) = weather_desc = observation_time = "N/A"
                alert_text = "No Alert"

                try:
                    print(f"[*] Scraping Current Data: {city}...")
                    response = requests.get(url, headers=headers, timeout=15)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "html.parser")

                    # 1. Alert (Peringatan)
                    alert_tag = soup.find("p", class_="truncate")
                    if alert_tag:
                        val = alert_tag.get_text(strip=True)
                        if val.lower() != "now":
                            alert_text = val

                    # 2. AQI & Status
                    aqi_container = soup.find(
                        "div", class_=lambda x: x and "aqi-bg-" in x
                    )
                    if aqi_container:
                        aqi_val_tag = aqi_container.find("p", class_="text-lg")
                        if aqi_val_tag:
                            aqi_val = aqi_val_tag.get_text(strip=True)

                        aqi_stat_tag = aqi_container.find(
                            "p", class_="font-body-l-medium"
                        )
                        if aqi_stat_tag:
                            aqi_stat = aqi_stat_tag.get_text(strip=True)

                    # 3. Main Pollutant & Concentration
                    info_div = soup.find("div", class_="font-body-m-medium")
                    if info_div:
                        ps = info_div.find_all("p")
                        if len(ps) >= 2:
                            pollutant = ps[1].get_text(strip=True)
                            conc = ps[-1].get_text(strip=True).replace("\xa0", " ")

                    # 4. Weather Details
                    weather_img = soup.find("img", alt="weather condition icon")

                    if weather_img:
                        icon_src = weather_img.get("src", "")
                        icon_file = icon_src.split("/")[-1]
                        try:
                            if "weather-" in icon_file:
                                icon_code = icon_file.split("weather-")[-1][:2]
                                weather_desc = weather_map.get(
                                    icon_code, f"Unknown ({icon_code})"
                                )
                        except Exception as e:
                            print(f"DEBUG: Error parsing icon code for {city}: {e}")

                        weather_container = weather_img.find_parent(
                            "div", class_=lambda x: x and "bg-white" in x
                        )
                        if weather_container:
                            p_elements = weather_container.find_all("p")
                            for p in p_elements:
                                text = p.get_text(strip=True)
                                if "°" in text:
                                    temp = text
                                elif "km/h" in text:
                                    wind = text
                                elif "%" in text:
                                    hum = text

                            img_wind = weather_container.find(
                                "img", alt="wind direction icon"
                            )
                            if img_wind and "style" in img_wind.attrs:
                                wind_direction = get_cardinal(img_wind["style"])
                    else:
                        print(f"[!] Weather icon not found for {city}")

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
                            print(f"DEBUG: Failed to parse time_info for {city}: {e}")

                    # 5. Append Data
                    current_data.append(
                        {
                            "province": province.replace("-", " ").title(),
                            "city": city.replace("-", " ").title(),
                            "aqi": aqi_val,
                            "aqi_status": aqi_stat,
                            "main_pollutant": pollutant,
                            "concentration": conc,
                            "weather": weather_desc,
                            "temperature": temp,
                            "humidity": hum,
                            "wind_speed": wind,
                            "wind_direction": wind_direction,
                            "alert": alert_text,
                            "observation_time": observation_time,
                            "scraped_at": datetime.now(local_tz).strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                        }
                    )

                except Exception as e:
                    print(f"[!] Error parsing item for {city}: {e}")

                time.sleep(5)

        # Output ke log Airflow sebagai Tabel
        if not current_data:
            return "No data scraped"

        df = pd.DataFrame(current_data)
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

        table_name = "raw_aqi_index"
        table_id = f"{tgt_namespace}.{table_name}"

        try:
            table = catalog.load_table(table_id)
            print(f"ℹ️ Table {table_id} found. Appending data...")

            with table.update_schema() as update:
                update.union_by_name(arrow_table.schema)
        except Exception:
            print(f"✨ Creating new table {table_id}...")
            table = catalog.create_table(
                identifier=table_id,
                schema=arrow_table.schema,
                location=f"s3a://lakehouse/{tgt_namespace}/{table_name}",
            )
        table.append(arrow_table)
        return f"🚀 Done! {len(current_data)} rows added to {table_id}"

    @task(outlets=[FORECAST_BRONZE])
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
                                        "province": province.replace("-", " ").title(),
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
            with table.update_schema() as update:
                update.union_by_name(arrow_table.schema)
        except Exception:
            print(f"✨ Creating new table {table_id}...")
            table = catalog.create_table(
                identifier=table_id,
                schema=arrow_table.schema,
                location=f"s3a://lakehouse/{tgt_namespace}/{table_name}",
            )
        table.append(arrow_table)
        return f"🚀 Done! {len(all_forecasts)} rows added to {table_id}"

    extract_aqi()
    extract_forecast_weather()


dag_obj = air_quality_and_forecast_weather()
