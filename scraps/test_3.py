import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import yaml


def get_air_quality_data(url, province, city):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Location Info
        country = (
            soup.find("a", href="/indonesia").get_text(strip=True)
            if soup.find("a", href="/indonesia")
            else "Indonesia"
        )
        city_tag = soup.find("h1").get_text(strip=True)
        found_city = city_tag.replace("Air quality in ", "").strip()

        # AQI Status & Pollutants
        aqi_status = (
            soup.find("p", class_="font-body-l-medium").get_text(strip=True)
            if soup.find("p", class_="font-body-l-medium")
            else "N/A"
        )

        main_pollutant, concentration = "N/A", "N/A"
        info_container = soup.find("div", class_="font-body-m-medium")
        if info_container:
            paragraphs = info_container.find_all("p")
            if len(paragraphs) >= 2:
                main_pollutant = paragraphs[1].get_text(strip=True)
                concentration = paragraphs[-1].get_text(strip=True)

        # Forecast Table Scraping
        forecast_table = soup.find("table")
        forecast_list = []
        if forecast_table:
            forecast_items = forecast_table.find_all("td")
            tracking_date = datetime.now()
            last_hour = -1
            ingest_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for item in forecast_items:
                try:
                    time_str = item.find("p", class_="max-w-12").get_text(strip=True)
                    current_hour = (
                        datetime.now().hour
                        if time_str.lower() == "now"
                        else int(time_str.split(":")[0])
                    )

                    if current_hour < last_hour:
                        tracking_date += timedelta(days=1)
                    last_hour = current_hour
                    full_ts = (
                        f"{tracking_date.strftime('%Y-%m-%d')} {current_hour:02d}:00:00"
                    )

                    aqi = item.find(
                        "div", class_=lambda x: x and "aqi-bg-" in x
                    ).get_text(strip=True)
                    temp = next(
                        (
                            p.text
                            for p in item.find_all("p", class_="font-medium")
                            if "°" in p.text
                        ),
                        "N/A",
                    )

                    metrics = item.find_all("div", class_="flex flex-col items-center")
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

                    forecast_list.append(
                        {
                            "country": country,
                            "province": province.replace("-", " ").title(),
                            "city": found_city,
                            "aqi": aqi,
                            "temp": temp,
                            "wind": wind,
                            "hum": hum,
                            "url": url,
                            "status": aqi_status,
                            "pollutant": main_pollutant,
                            "conc": concentration,
                            "ingest_timestamp": ingest_ts,
                            "data_timestamp": full_ts,
                        }
                    )
                except Exception:
                    continue

        return forecast_list
    except Exception as e:
        print(f"Error scraping: {e}")
        return []


def print_air_quality_table(data):
    """Fungsi khusus untuk menangani tampilan output terminal"""
    if not data:
        print("No data available to display.")
        return

    header = (
        f"{'country':<10} | {'province':<12} | {'city':<12} | {'aqi':<4} | "
        f"{'status':<10} | {'pollutant':<10} | {'concentration':<13} | {'temperature':<11} | "
        f"{'wind':<4} | {'humidity':<8} | {'URL':<55} | {'data_ts':<22} | {'ingest_ts'} "
    )

    print(f"\n=== AIR QUALITY REPORT & HOURLY FORECAST {data[0]['city'].upper()} ===\n")
    print(header)
    print("-" * len(header))

    for i, row in enumerate(data):
        # Logika: Hanya tampilkan info detail di baris pertama
        disp_status = row["status"] if i == 0 else ""
        disp_pollutant = row["pollutant"] if i == 0 else ""
        disp_conc = row["conc"] if i == 0 else ""

        print(
            f"{row['country']:<10} | "
            f"{row['province']:<12} | "
            f"{row['city']:<12} | "
            f"{row['aqi']:<4} | "
            f"{disp_status:<10} | "
            f"{disp_pollutant:<10} | "
            f"{disp_conc:<13} | "
            f"{row['temp']:<11} | "
            f"{row['wind']:<4} | "
            f"{row['hum']:<8} | "
            f"{row['url']:<55} | "
            f"{row['data_timestamp']:<22} | "
            f"{row['ingest_timestamp']:<22} "
        )


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # 1. Load Parameter dari YAML
    try:
        with open("./scraps/config.yaml", "r") as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        print("Error: config.yaml tidak ditemukan!")
        exit()

    for loc in config["locations"]:
        province = loc["province"]

        for city in loc["cities"]:
            target_url = f"https://www.iqair.com/indonesia/{province}/{city}"
            print(f"Mengambil data untuk: {city.upper()}, {province.upper()}...")

            results = get_air_quality_data(target_url, province, city)

            if results:
                print_air_quality_table(results)
            else:
                print(f"Gagal mendapatkan data untuk {city}\n")

    print("=== SEMUA PROSES SELESAI ===")
