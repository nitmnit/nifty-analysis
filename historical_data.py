import csv
import os
import setup_env  # noqa
from logger_settings import logger
from kiteconnect import KiteConnect
import config
from urllib.parse import urlparse, parse_qs
from typing import Optional
from selenium import webdriver
from time import sleep
from datetime import datetime, timedelta
from selenium.webdriver.chrome.options import Options


class KiteUtil:
    def __init__(self):
        self.kite = KiteConnect(api_key=config.KITE_API_KEY)
        chrome_options = Options()
        chrome_options.add_argument('--user-data-dir=./user-data/')
        # desired_capabilities['sessionName'] = 'MyPersistentSession'  # Replace with a unique name
        with webdriver.Chrome(options=chrome_options) as driver:
            driver.get(self.kite.login_url())
            while "nitinsuresh.me" not in driver.current_url:
                sleep(1)
            request_token = self.get_params(driver.current_url, "request_token")
            data = self.kite.generate_session(request_token, api_secret=config.KITE_SECRET)
            self.kite.set_access_token(data["access_token"])
        self.instruments = {}
        for instrument in self.kite.instruments('NSE'):
            self.instruments[instrument["tradingsymbol"]] = instrument

    def get_params(self, url: str, param_name: str) -> Optional[str | None]:
        parsed_url = urlparse(url)  # Parse the URL
        query_string = parsed_url.query  # Extract the query string
        params = parse_qs(query_string)
        return params.get(param_name)[0]

    def fetch_stock_data(self, symbol: str, from_date: datetime, to_date: datetime) -> None:
        logger.info(f"requesting {symbol}, from: {from_date}, to: {to_date}")
        try:
            data = self.kite.historical_data(self.instruments[symbol]["instrument_token"], from_date, to_date, "minute")
        except Exception as e:
            logger.error(e)
            sleep(1)
            data = self.kite.historical_data(self.instruments[symbol]["instrument_token"], from_date, to_date, "minute")
        return data

    def fetch_data(self) -> None:
        for symbol in config.option_stocks:
            current_from_date = datetime.strptime(config.DATE_START, "%Y-%m-%d")
            today = datetime.now()
            while current_from_date < today:
                file_path = f"data/{symbol}-NSE-{current_from_date.strftime('%Y-%m-%d')}.csv"
                if not os.path.exists(file_path):
                    data = self.fetch_stock_data(symbol, current_from_date, current_from_date + timedelta(hours=24))
                    with open(file_path, "w", newline='') as csvfile:
                        if data:
                            headers = list(data[0].keys())
                            writer = csv.DictWriter(csvfile, fieldnames=headers)
                            writer.writeheader()
                            for row in data:
                                writer.writerow(row)
                current_from_date += timedelta(hours=24)


class DataUtil:
    @staticmethod
    def get_intraday_csv_path(symbol: str, date: datetime) -> str:
        file_path = f"data/{symbol}-NSE-{date.strftime('%Y-%m-%d')}.csv" 
        if not os.path.exists(file_path):
            raise Exception("file not found")
        return file_path

# x = KiteUtil()
# x.fetch_data()
