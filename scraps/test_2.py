import requests
from bs4 import BeautifulSoup
from datetime import datetime


def get_air_quality_forecast(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        pretty_html = soup.prettify()
        # print(pretty_html)

        now = datetime.now().strftime("%Y-%m-%d-%H%M")
        file_name = f"hasil_scraping_{now}.html"
        with open(file_name, "w", encoding="utf-8") as file:
            file.write(pretty_html)

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    target_url = "https://www.iqair.com/indonesia/jakarta/jakarta"
    get_air_quality_forecast(target_url)
