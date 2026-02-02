import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


def get_air_quality(url, province, city):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Web Data Datetime
        h2_element = soup.find("h2")
        if not h2_element:
            print("h2 element failed .")
            return

        time_info = (
            h2_element.get_text(strip=True)
            .split("•")[-1]
            .replace("Local time", "")
            .strip()
        )

        now = datetime.now()
        year = now.year
        if now.month == 1 and "Dec" in time_info:
            year -= 1

        convert_dt = datetime.strptime(f"{time_info} {year}", "%H:%M, %b %d %Y")

        # Location : Country
        country = soup.find("a", href="/indonesia").get_text(strip=True)

        # Location : City
        city_tag = soup.find("h1").get_text(strip=True)
        city = city_tag.replace("Air quality in ", "").strip()

        # 1. AQI Value
        aqi_value = "N/A"
        aqi_box = soup.find("p", class_="text-lg font-medium")
        if aqi_box:
            aqi_value = aqi_box.get_text(strip=True)

        # 2. AQI Status
        aqi_status = "N/A"
        status_box = soup.find("p", class_="font-body-l-medium")
        if status_box:
            aqi_status = status_box.get_text(strip=True)

        # 3. Main Pollutant & Concentration
        main_pollutant = "N/A"
        concentration = "N/A"

        info_container = soup.find("div", class_="font-body-m-medium")
        if info_container:
            paragraphs = info_container.find_all("p")
            if len(paragraphs) >= 2:
                # Berdasarkan HTML Anda: p[0] = label, p[1] = PM2.5, p[2] = nilai
                # Namun kita ambil teks terakhir untuk konsentrasi
                main_pollutant = paragraphs[1].get_text(strip=True)
                concentration = paragraphs[-1].get_text(strip=True)

        print("=" * 30)
        print("   AIR QUALITY REPORT   ")
        print("=" * 30)
        print(f"url                     : {url}")
        print(f"Web Data Datetime       : {convert_dt}")
        print(
            f"Current Datetime        : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        print(f"Country                 : {country}")
        print(f"Province                : {province.replace('-', ' ').title()}")
        print(f"City                    : {city}")
        print(f"AQI Index               : {aqi_value}")
        print(f"Status                  : {aqi_status}")
        print(f"Main Pollutant          : {main_pollutant}")
        print(f"Concentration           : {concentration}")
        print("=" * 30)

    except Exception as e:
        print(f"Failed: {e}")


def get_air_quality_forecast(url, province, city):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        print(f"=== HOURLY FORECAST {city.upper()} ===")

        # 1. forecast
        forecast_table = soup.find("table")
        if not forecast_table:
            print("Tabel forecast tidak ditemukan.")
            return

        forecast_items = forecast_table.find_all("td")

        print(
            f"\n{'TIMESTAMP':<18} | {'AQI':<4} | {'TEMP':<5} | {'WIND':<6} | {'HUM':<4}"
        )
        print("-" * 55)
        tracking_date = datetime.now()
        last_hour = -1

        # --- Bagian Loop yang diperbarui ---
        for item in forecast_items:
            try:
                time_str = item.find("p", class_="max-w-12").get_text(strip=True)

                # 1. Tentukan jam (integer) untuk logika pindah hari
                if time_str.lower() == "now":
                    current_hour = datetime.now().hour
                    display_time = datetime.now().strftime(
                        "%H:00:00"
                    )  # "Now" jadi jam genap
                else:
                    current_hour = int(time_str.split(":")[0])
                    display_time = f"{current_hour:02d}:00:00"

                # 2. Logika pindah hari
                if current_hour < last_hour:
                    tracking_date += timedelta(days=1)
                last_hour = current_hour

                # 3. GABUNGKAN TANGGAL & JAM
                # Kita ambil YYYY-MM-DD dari tracking_date dan jam dari display_time
                full_timestamp = f"{tracking_date.strftime('%Y-%m-%d')} {display_time}"

                # --- Ekstraksi Data Lainnya (Tetap Sama) ---
                aqi = item.find("div", class_=lambda x: x and "aqi-bg-" in x).get_text(
                    strip=True
                )
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

                # --- PRINT DENGAN FORMAT BARU ---
                print(
                    f"{full_timestamp:<18} | {aqi:<4} | {temp:<5} | {wind:<6} | {hum:<4}"
                )

            except Exception:
                continue

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    province = "west-java"
    city = "bandung"
    target_url = f"https://www.iqair.com/indonesia/{province}/{city}"
    get_air_quality(target_url, province, city)
    get_air_quality_forecast(target_url, province, city)
