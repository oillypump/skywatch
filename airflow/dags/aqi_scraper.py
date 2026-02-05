from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
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


@dag(
    dag_id="aqi_scraper",
    schedule="10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "aqi"],
)
def air_quality_index():
    @task()
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
                import re

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
        if current_data:
            df = pd.DataFrame(current_data)
            # print("\n" + "=" * 80 + "\nREPORT AIR QUALITY\n" + "=" * 80)
            # print(df.to_string(index=False))
            # return f"Successful for {len(df)} cities."
            s3_client = boto3.client(
                "s3",
                endpoint_url="http://minio:9000",
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin",
                config=Config(signature_version="s3v4"),
            )
            bucket_name = "lakehouse"
            now = datetime.now(local_tz)
            file_name = f"aqi_scraper_{now.strftime('%Y%m%d_%H%M')}.csv"
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
            print(df.to_string(index=False))
        return f"Successful: {len(df)} cities uploaded to {object_key}."

    extract_aqi()


dag_obj = air_quality_index()
