import requests
import os
from bs4 import BeautifulSoup


def debug_html(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        print(f"[*] Fetching: {url}")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        print("*")
        print("*")
        print("*")
        weather_bar = soup.find("div", class_="bg-white")
        print(weather_bar)
        print("*")
        print("*")
        print("*")
        weather_img = soup.find("img", alt="weather condition icon")
        print(weather_img)
        print("*")
        print("*")
        print("*")
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
        print(time_info)
        print("*")
        print("*")
        print("*")

    except Exception as e:
        print(f"[!] Gagal: {e}")


# Contoh penggunaan untuk cek Tanah Abang atau Depok
target_url = "https://www.iqair.com/indonesia/jakarta/jakarta"
debug_html(target_url)
