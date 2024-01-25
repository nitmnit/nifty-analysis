import requests
from datetime import datetime, timedelta
import json
from logger_settings import logger

# Settings start
from_date_str = "2023-07-01"
to_date_str = "2024-01-25"
from_date_dt = datetime.strptime(from_date_str, "%Y-%m-%d")
to_date_dt = datetime.strptime(to_date_str, "%Y-%m-%d")
# Settings End


class NSEUtil:
    AN_URL = "https://www.nseindia.com/api/corporate-announcements"
    PERIOD_DAYS = 2
    DATE_FORMAT = "%d-%m-%Y"

    def __init__(self) -> None:
        self.session = requests.Session()
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
        logger.info("created session")

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
            self.session.get(self.AN_URL)
            logger.info(f"sending params: {params}, cookies: {self.session.cookies}")
            response = self.session.get(self.AN_URL, params=params)
            if not response.ok:
                logger.error(response.text)
                raise Exception("request rejected")
            results.append(response.json())
            current_from_date = current_from_date + timedelta(days=self.PERIOD_DAYS)
            logger.info("success getting announcements")
        return results


x = NSEUtil()
y = x.fetch_fo_notifications(from_date_dt, to_date_dt)
with open("announcements.json", "w+") as f:
    f.write(json.dumps(y))
