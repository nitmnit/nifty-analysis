import requests
from datetime import datetime, timedelta
import json
from logger_settings import logger

# Settings start
# from_date_str = "2023-07-01"
from_date_str = "2020-01-01"
to_date_str = "2024-01-25"
from_date_dt = datetime.strptime(from_date_str, "%Y-%m-%d")
to_date_dt = datetime.strptime(to_date_str, "%Y-%m-%d")
# Settings End


class NSEUtil:
    HOME_URL = (
        "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
    )
    AN_URL = "https://www.nseindia.com/api/corporate-announcements"
    PERIOD_DAYS = 2
    DATE_FORMAT = "%d-%m-%Y"

    def __init__(self) -> None:
        self.session = requests.Session()
        logger.info("session create start")
        self.session.headers.update(
            {
                "authority": "www.nseindia.com",
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "dnt": "1",
                "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Brave";v="120"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Linux"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "sec-gpc": "1",
                "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            }
        )
        self.session.get(self.HOME_URL)
        logger.info("session created")

    def fetch_fo_notifications(self, from_date, to_date):
        results = []
        current_from_date = from_date
        while current_from_date < to_date:
            params = {
                "index": "equities",
                "from_date": current_from_date.strftime(self.DATE_FORMAT),
                "to_date": (
                    current_from_date + timedelta(days=self.PERIOD_DAYS)
                ).strftime(self.DATE_FORMAT),
                "fo_sec": "true",
            }
            logger.info(f"sending params: {params}")
            response = self.session.get(self.AN_URL, params=params)
            if not response.ok:
                logger.error(response.text)
                raise Exception("request rejected")
            results += response.json()
            current_from_date = current_from_date + timedelta(days=self.PERIOD_DAYS + 1)
            logger.info("success getting announcements")
        return results


x = NSEUtil()
y = x.fetch_fo_notifications(from_date_dt, to_date_dt)
with open("announcements.json", "w+") as f:
    f.write(json.dumps(y, indent=4))


# curl 'https://www.nseindia.com/api/corporate-announcements?index=equities&from_date=23-01-2024&to_date=25-01-2024&fo_sec=true' \
#   -H 'authority: www.nseindia.com' \
#   -H 'accept: */*' \
#   -H 'accept-language: en-US,en;q=0.9' \
#   -H 'cookie: ak_bmsc=E0F73B9ACB9E69348575FA2C87AEDDAC~000000000000000000000000000000~YAAQSjLUF+/llzeNAQAAzM0AQBb3nP/sp8RI6NiGtmCCyDlRatJ4GP4/50DQ0ELesPKXFQXzoM3rGo8Fq5I8waP8iPuOOANLQeKj0I5RsHYJ6yhhlZ7ilgIM9PBPJ86oo6cJk2DIMGmjM+mgUaIh130Bt0ybu3kJf3+jEBR60PPz1MXyLLzhVqduFlycM3OQ5uPJXGRqiZDtZSoCX/rxpgv8+FvqjsChkMmFbCOwkxGg8M70tj4FEkEFlH69qOjMf2G/ZMx6nbL5nx644MpiykdkG5CJJzSEhYrEj6uOdhWRDD8SjYPsVOXOk9bzPjmUF0aJLh6DmB8CYIeuCiys0Qe9KaHq1myQjB1Fulvp3ZsPWv+SgM9dxNySLS/t383Cc2IWoPB8oUYfAh8=; nsit=LeAFGo27Y7fFG-yUnmF8XMYu; AKA_A2=A; defaultLang=en; bm_mi=B238AB146D37EAEDEA92C0907625C20F~YAAQhzLUFzBjshyNAQAAaXQcQBbyA9k6ZJ/rd2uizTu17uMGsmHg2m/6spPsEFJKfN5F2JJ0KLE8/pxRSQ5ncMRIHTFIaI20Al8fRZkXtizPpBo7tWM3RpyNeLSQpkCHomiUi+84YlJXbVpi/ohAJjca2EA4ZJmThKs4NLBAQBmpj6eaC8GyGCrAJ8+0tAGRDg5uQG5aih/F1SO46gx2FtmEyx3ky/jXnwAHQkS+CAsID9LKevhi1Vw+J/hAtcWhl22bZ3cbc1d/Oyvtq5OQTDvqAyRVV1DA6GH8MTL9HMgBYu005OTKeRi1bW/SnklmAAetTOBGpPKvamJwLQCWug+hmBw6U2j946BHbhqy+YWsylxUyY+wjatpr7Vv7f1u~1; nseappid=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTcwNjE3NzYyNSwiZXhwIjoxNzA2MTg0ODI1fQ.aAiPeih8Z70FumTC8LV8zS1A4hoMMUv187-rOcKxWLU; bm_sv=94BF2DF3DE46F2BE1C493C53D248F4C4~YAAQhzLUF7JjshyNAQAA6IocQBbeCX/BnJlvRkHNRk5/Fbdc+FylFMJYkzygJMFM36JKpDJVd67jH655XDZujQt7310uP9LJX8ftNY8p3kw8Ycn2Wh94IaEanQsqwAazhXpR+YPeOqeYVuqMBTHuWflW8jiyXSXommkSzYWw8Fgsp/gd4qRmU7/n8eY4PH+40EaL+E9c/JbjzpaUE/g3M/cS2JmYEjRe+FIA2OkUhwpxTf/csSK/mTOIsxSk2GtJez5L~1' \
#   -H 'dnt: 1' \
#   -H 'referer: https://www.nseindia.com/companies-listing/corporate-filings-announcements' \
#   -H 'sec-ch-ua: "Not_A Brand";v="8", "Chromium";v="120", "Brave";v="120"' \
#   -H 'sec-ch-ua-mobile: ?0' \
#   -H 'sec-ch-ua-platform: "Linux"' \
#   -H 'sec-fetch-dest: empty' \
#   -H 'sec-fetch-mode: cors' \
#   -H 'sec-fetch-site: same-origin' \
#   -H 'sec-gpc: 1' \
#   -H 'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' \
#   --compressed
