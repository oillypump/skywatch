"""
Module scraping AQI + weather dari iqair.com, lalu load ke Snowflake (layer BRONZE).

Dipisah dari file DAG supaya:
1. DAG file (dags/*.py) tetap ringkas — hanya berisi orkestrasi (schedule, retry, dependency)
2. Logic scraping bisa di-testing terpisah tanpa perlu spin up Airflow
3. Kalau nanti mau dipindah ke container terpisah, tinggal copy file ini apa adanya

Semua parameter koneksi (conn_id, database, schema, table) SENGAJA tidak di-hardcode
di sini — dikirim dari DAG supaya module ini reusable dan config-nya cuma ada
di satu tempat (DAG file).
"""

import logging
import os
import re
import time
from datetime import datetime

import pendulum
import pandas as pd
import yaml
import requests
from bs4 import BeautifulSoup
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}


def build_create_table_sql(database: str, schema: str, table: str) -> str:
    """
    Helper untuk generate DDL create table. Dipanggil dari DAG (untuk
    SQLExecuteQueryOperator) maupun dari create_table() di module ini,
    supaya definisi kolom cuma ada di SATU tempat.
    """
    return f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} (
            PROVINCE         VARCHAR(100),
            CITY             VARCHAR(100),
            AQI              VARCHAR(20),
            AQI_STATUS       VARCHAR(50),
            MAIN_POLLUTANT   VARCHAR(50),
            CONCENTRATION    VARCHAR(50),
            WEATHER          VARCHAR(100),
            TEMPERATURE      VARCHAR(20),
            HUMIDITY         VARCHAR(20),
            WIND_SPEED       VARCHAR(20),
            WIND_DIRECTION   VARCHAR(50),
            ALERT            VARCHAR(500),
            OBSERVATION_TS   TIMESTAMP_NTZ,
            SCRAPED_TS       TIMESTAMP_NTZ
        )
    """


def _load_config() -> dict:
    """
    Cari config.yaml sejajar dengan file aqi_scraper.py ini sendiri
    (bukan relatif ke file DAG yang memanggilnya).
    """
    module_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(module_dir, "config.yaml")

    if not os.path.exists(config_path):
        logger.error(f"File config.yaml tidak ditemukan: {config_path}")
        raise FileNotFoundError(f"File config.yaml tidak ditemukan: {config_path}")

    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def _get_cardinal(style_str: str) -> str:
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
    except Exception as e:
        logger.warning(f"Gagal parse cardinal direction dari '{style_str}': {e}")
        return "N/A"


def _parse_observation_ts(raw_str: str, local_tz):
    try:
        cleaned = raw_str.replace(".", ":").strip()
        match = re.match(r"(\d{1,2}:\d{2}),\s*(\w{3})\s*(\d{1,2})", cleaned)
        if not match:
            return None

        time_part, month_str, day_str = match.groups()
        hour, minute = (int(x) for x in time_part.split(":"))
        month_num = datetime.strptime(month_str, "%b").month
        year = datetime.now(local_tz).year

        dt = pendulum.datetime(year, month_num, int(day_str), hour, minute, tz=local_tz)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    except Exception as e:
        logger.warning(f"Gagal parse observation_ts '{raw_str}': {e}")
        return None


def _scrape_city(province: str, city: str, weather_map: dict, local_tz) -> dict:
    url = f"https://www.iqair.com/indonesia/{province}/{city}"

    aqi_val = aqi_stat = pollutant = conc = temp = wind = hum = wind_direction = (
        weather_desc
    ) = observation_ts_raw = "N/A"
    alert_text = "No Alert"

    try:
        logger.info(f"[*] Scraping Current Data: {city}...")
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        alert_tag = soup.find("p", class_="truncate")
        if alert_tag:
            val = alert_tag.get_text(strip=True)
            if val.lower() != "now":
                alert_text = val

        aqi_container = soup.find("div", class_=lambda x: x and "aqi-bg-" in x)
        if aqi_container:
            aqi_val_tag = aqi_container.find("p", class_="text-lg")
            if aqi_val_tag:
                aqi_val = aqi_val_tag.get_text(strip=True)

            aqi_stat_tag = aqi_container.find("p", class_="font-body-l-medium")
            if aqi_stat_tag:
                aqi_stat = aqi_stat_tag.get_text(strip=True)

        info_div = soup.find("div", class_="font-body-m-medium")
        if info_div:
            ps = info_div.find_all("p")
            if len(ps) >= 2:
                pollutant = ps[1].get_text(strip=True)
                conc = ps[-1].get_text(strip=True).replace("\xa0", " ")

        weather_img = soup.find("img", alt="weather condition icon")
        if weather_img:
            icon_src = weather_img.get("src", "")
            icon_file = icon_src.split("/")[-1]
            try:
                if "weather-" in icon_file:
                    icon_code = icon_file.split("weather-")[-1][:2]
                    weather_desc = weather_map.get(icon_code, f"Unknown ({icon_code})")
            except Exception as e:
                logger.debug(f"Error parsing icon code for {city}: {e}")

            weather_container = weather_img.find_parent(
                "div", class_=lambda x: x and "bg-white" in x
            )
            if weather_container:
                for p in weather_container.find_all("p"):
                    text = p.get_text(strip=True)
                    if "°" in text:
                        temp = text
                    elif "km/h" in text:
                        wind = text
                    elif "%" in text:
                        hum = text

                img_wind = weather_container.find("img", alt="wind direction icon")
                if img_wind and "style" in img_wind.attrs:
                    wind_direction = _get_cardinal(img_wind["style"])
        else:
            logger.warning(f"[!] Weather icon not found for {city}")

        h2_tag = soup.find("h2")
        if h2_tag:
            h2_text = h2_tag.get_text(strip=True)
            match = re.search(r"(\d{1,2}[:.]\d{2},\s\w{3}\s\d{1,2})", h2_text)
            observation_ts_raw = match.group(1) if match else h2_text[-15:].strip()

    except Exception as e:
        logger.warning(f"[!] Error parsing item for {city}: {e}")

    return {
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
        "observation_ts": _parse_observation_ts(observation_ts_raw, local_tz),
        "scraped_ts": datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S"),
    }


def run(conn_id: str, database: str, schema: str, table: str) -> None:
    """
    Entry point untuk task scrape + load.
    Asumsi: table target SUDAH ada (dibuat lewat task create_table di DAG,
    pakai SQLExecuteQueryOperator + build_create_table_sql()).

    Semua parameter koneksi dikirim dari DAG, tidak di-hardcode di sini.
    """
    cfg = _load_config()
    weather_map = cfg.get("weather_map", {})
    locations = cfg.get("locations", [])
    local_tz = pendulum.timezone("Asia/Jakarta")

    current_data = []
    for loc in locations:
        province = loc["province"]
        for city in loc["cities"]:
            current_data.append(_scrape_city(province, city, weather_map, local_tz))
            time.sleep(5)

    if not current_data:
        logger.warning("Tidak ada data yang berhasil di-scrape.")
        return

    df = pd.DataFrame(current_data)
    df.columns = [c.upper() for c in df.columns]
    logger.info("Total %d baris siap di-load.", len(df))

    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()

    try:
        success, num_chunks, num_rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table,
            database=database,
            schema=schema,
            auto_create_table=False,
            overwrite=False,
        )

        if success:
            logger.info(
                "Berhasil insert %d baris ke %s.%s.%s (%d chunk).",
                num_rows,
                database,
                schema,
                table,
                num_chunks,
            )
        else:
            raise RuntimeError(f"write_pandas gagal untuk tabel {table}")

    finally:
        conn.close()
