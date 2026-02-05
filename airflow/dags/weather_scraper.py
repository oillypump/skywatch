from airflow.decorators import dag, task  # Perbaikan import
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


@dag(
    dag_id="weather_scraper",
    schedule="15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "weather"],
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
                    response = requests.get(url, headers=headers, timeout=15)
                    response.raise_for_status()  # Cek jika status bukan 200
                    soup = BeautifulSoup(response.text, "html.parser")

                    forecast_table = soup.find("table")
                    if forecast_table:
                        forecast_items = forecast_table.find_all("td")
                        tracking_date = datetime.now(local_tz)
                        last_hour = -1
                        ingest_ts = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")

                        for item in forecast_items:
                            try:
                                print(f"[*] Scraping Current Data: {city}...")
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

        # --- 3. REPORTING ---
        if all_forecasts:
            df = pd.DataFrame(all_forecasts)
            print(f"\nSUCCESS: Scraped {len(df)} rows.")
            print(df.head(5).to_string())
            # return all_forecasts

            s3_client = boto3.client(
                "s3",
                endpoint_url="http://minio:9000",
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin",
                config=Config(signature_version="s3v4"),
            )
            bucket_name = "lakehouse"
            now = datetime.now(local_tz)
            file_name = f"forecast_weather_scraper_{now.strftime('%Y%m%d_%H%M')}.csv"
            object_key = f"bronze/{file_name}"

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            try:
                s3_client.put_object(
                    Bucket=bucket_name, Key=object_key, Body=csv_buffer.getvalue()
                )
                print(f"\n[OK] Data uploaded to MinIO: {bucket_name}/{object_key}")
            except Exception as minio_err:
                print(f"\n[!] MinIO Upload Error: {minio_err}")

            # Print report ke log
            print("\n" + "=" * 80 + "\nREPORT AIR QUALITY\n" + "=" * 80)
            # print(df.to_string(index=False))
        return f"Successful: {len(df)} cities uploaded to {object_key}."

    # Memanggil task di dalam DAG
    extract_forecast_weather()


# Inisialisasi DAG
dag_instance = forecast_weather()
