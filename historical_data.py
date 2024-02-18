import csv
import os
import random
import setup_env  # noqa
from logger_settings import logger
from kiteconnect import KiteConnect
from kiteconnect.exceptions import TokenException
import config
from urllib.parse import urlparse, parse_qs
from typing import Optional, List
from selenium import webdriver
from time import sleep
from datetime import datetime, timedelta
from selenium.webdriver.chrome.options import Options
import pandas as pd
import threading
from constants import *


#lock = threading.Lock()


class KiteUtil:
    ACCESS_TOKEN_FILE = "ACCESS_TOKEN"
    BATCH_SIZE_DAYS = 60

    def __init__(self, exchange=EXCHANGE_NSE):
        self.kite = KiteConnect(api_key=config.KITE_API_KEY)
        self.access_token = self.set_access_token()
        self.instruments = {}
        self.instruments_list = self.kite.instruments(exchange)
        self.exchange = exchange
        self.fetch_instruments(exchange=exchange)
    
    def fetch_instruments(self, exchange):
        for instrument in self.kite.instruments(exchange):
            self.instruments[instrument["tradingsymbol"]] = instrument
    
    def get_fo_instrument(self, symbol, expiry, strike_price, option_type):
        for instrument in self.instruments_list:
            #print(f'ins nm: {instrument["name"]} ex: {instrument["expiry"]}, seg: {instrument["segment"]}, st: {instrument["strike"]} t: {instrument["instrument_type"]}')
            if instrument["name"] == symbol and instrument["expiry"] == expiry and instrument["segment"] == "NFO-OPT" and instrument["strike"] == strike_price and instrument["instrument_type"] == f"{option_type}E":
                return instrument

    def get_nse_instrument_token(self, symbol):
        for instrument in self.kite.instruments(exchange=EXCHANGE_NSE):
            if instrument["name"] == symbol:
                return instrument["instrument_token"]

    def fetch_access_token(self):
        chrome_options = Options()
        chrome_options.add_argument('--user-data-dir=./user-data/')
        with webdriver.Chrome(options=chrome_options) as driver:
            driver.get(self.kite.login_url())
            while "nitinsuresh.me" not in driver.current_url:
                sleep(1)
            request_token = self.get_params(driver.current_url, "request_token")
            data = self.kite.generate_session(request_token, api_secret=config.KITE_SECRET)
        return data["access_token"]

    def set_access_token(self):
        success = False
        if os.path.exists(self.ACCESS_TOKEN_FILE):
            with open(self.ACCESS_TOKEN_FILE, 'r') as f:
                access_token = f.read()
            self.kite.set_access_token(access_token)
            try:
                # Testing here to make sure that it's not expired token
                self.kite.profile()
            except TokenException as e:
                logger.info("access_token expired, creating new token")
            else:
                success = True
            if success:
                return access_token
        access_token = self.fetch_access_token()
        with open(self.ACCESS_TOKEN_FILE, 'w') as f:
            f.write(access_token)
        self.kite.set_access_token(access_token)
        return access_token

    def get_params(self, url: str, param_name: str) -> Optional[str | None]:
        parsed_url = urlparse(url)  # Parse the URL
        query_string = parsed_url.query  # Extract the query string
        params = parse_qs(query_string)
        return params.get(param_name)[0]

    def fetch_stock_data(self, symbol: str, from_date: datetime, to_date: datetime, interval) -> None:
        logger.info(f"requesting {symbol}, interval: {interval}, from: {from_date}, to: {to_date}")
        try:
            data = self.kite.historical_data(self.instruments[symbol]["instrument_token"], from_date, to_date, interval)
        except Exception as e:
            logger.error(e)
            sleep(round(4 * random.random()))
            data = self.kite.historical_data(self.instruments[symbol]["instrument_token"], from_date, to_date, interval)
        return data

    @staticmethod
    def get_file_path(symbol: str, date: datetime, exchange, interval) -> str:
        if interval != INTERVAL_DAY:
            return f"data/{symbol}/{exchange}/{interval}/{date.strftime('%Y-%m-%d')}.csv"
        else:
            return f"data/{symbol}/{exchange}/{interval}/{date.strftime('%Y')}.csv"

    def fetch_nifty_data(self, interval) -> None:
        symbols = [symbol for symbol in self.instruments.keys() if symbol.startswith("NIFTY")]
        self.fetch_bulk_data(symbols, interval)

    def fetch_bulk_data(self, symbols: List[str], interval) -> None:
        today = datetime.now()
        removed = 0
        for symbol in symbols:
            current_from_date = datetime.strptime(config.DATE_START, "%Y-%m-%d")
            while current_from_date < today:
                if interval == INTERVAL_DAY:
                    cur_to_date = current_from_date.replace(year=current_from_date.year + 1)
                else:
                    cur_to_date = current_from_date + timedelta(days=MAX_PERIOD[interval])
                file_path = self.get_file_path(symbol, current_from_date, exchange="NSE", interval=interval)
                if not os.path.exists(file_path):
                    data = self.fetch_stock_data(symbol, current_from_date, cur_to_date, interval)
                    df = pd.DataFrame(data)
                    if df.shape[0] != 0:
                        df.set_index("date", inplace=True)
                        if interval != INTERVAL_DAY:
                            while current_from_date < cur_to_date and current_from_date < today:
                                cur_date_df = df[df.index.date == current_from_date.date()]
                                file_path = self.get_file_path(symbol, current_from_date.date(), exchange="NSE", interval=interval)
                                try:
                                    cur_date_df.to_csv(file_path)
                                except OSError:
                                    os.makedirs(file_path.rsplit('/', 1)[0])
                                    cur_date_df.to_csv(file_path)
                                current_from_date += timedelta(days=1)
                        else:
                            file_path = self.get_file_path(symbol, current_from_date.date(), exchange="NSE", interval=interval)
                            try:
                                df.to_csv(file_path)
                            except OSError:
                                os.makedirs(file_path.rsplit('/', 1)[0])
                                df.to_csv(file_path)
                            current_from_date = cur_to_date
                    else:
                        while current_from_date < cur_to_date and current_from_date < today:
                            file_path = self.get_file_path(symbol, current_from_date.date(), exchange="NSE", interval=interval)
                            try:
                                df.to_csv(file_path)
                            except OSError:
                                os.makedirs(file_path.rsplit('/', 1)[0])
                                df.to_csv(file_path)
                            current_from_date += timedelta(days=1)
                        current_from_date = cur_to_date
                else:
                    if current_from_date > today:
                        os.remove(file_path)
                        removed += 1
                        logger.info(f"removed: {removed}")
                    if interval != INTERVAL_DAY:
                        current_from_date += timedelta(days=1)
                    else:
                        current_from_date = cur_to_date


if __name__ == "__main__":
    splits = 5
    x = KiteUtil()
    threadpool = []
    offset = 0
    limit = len(config.option_stocks)
    all_symbols = config.option_stocks
    nifty_symbols = [symbol for symbol in x.instruments.keys() if symbol.startswith("NIFTY")]
    all_symbols = all_symbols + nifty_symbols
    for interval in ALL_INTERVALS:
        print(interval)
        while offset < limit:
            stocks = all_symbols[offset : offset+limit//splits]
            thread = threading.Thread(target=x.fetch_bulk_data, args=(stocks, interval))
            threadpool.append(thread)
            thread.start()
            offset += limit//splits
        for thread in threadpool:
            thread.join()
        offset = 0
    #print(x.instruments.keys())
    #x.fetch_data(INTERVAL_MIN10)
    #x.fetch_nifty_data()
    #x.fetch_nifty_data(interval=INTERVAL_MIN15)
    #x.fetch_nifty_data(interval=INTERVAL_MIN10)
    #x.fetch_nifty_data(interval=INTERVAL_DAY)
    #from_date_str = "2023-07-01"
    #from_date_dt = datetime.strptime(from_date_str, "%Y-%m-%d")
    #period = 365 * 15
    #to_date = from_date_dt + timedelta(days=period)


    #print(x.fetch_stock_data("RELIANCE", from_date=from_date_dt, to_date=to_date, interval=INTERVAL_DAY,))
