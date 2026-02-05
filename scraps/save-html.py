import requests
import os
from bs4 import BeautifulSoup


def save_page_html(url, filename="debug_page.html"):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        print(f"[*] Fetching: {url}")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        forecast_table = soup.find("table")
        content = forecast_table.prettify()
        # Menulis konten ke file lokal
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)

        print(f"[+] Berhasil! File disimpan di: {os.path.abspath(filename)}")

    except Exception as e:
        print(f"[!] Gagal: {e}")


# Contoh penggunaan untuk cek Tanah Abang atau Depok
target_url = "https://www.iqair.com/indonesia/jakarta/jakarta"
save_page_html(target_url, "cek_iqair_jakarta.html")
