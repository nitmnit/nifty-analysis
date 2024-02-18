from historical_data import KiteUtil
import pandas as pd
import datetime as dt
import icharts
from icharts_config import expiries
from functools import cache
from constants import *


@cache
def has_data(symbol, candle_timestamp, interval, exchange):
    file_path = KiteUtil.get_file_path(symbol, candle_timestamp, exchange=exchange, interval=interval)
    try:
        df = pd.read_csv(file_path, index_col="date", parse_dates=True)
    except (pd.errors.EmptyDataError, FileNotFoundError):
        print(f"file not found or empty dateframe for symbol: {symbol} on date: {candle_timestamp}, file_path: {file_path}")
        return False, None
    return df.shape[0] != 0, df

@cache
def get_last_trading_day(symbol, date, interval, exchange):
    cur_date = date
    data_available = False
    while not data_available:
        cur_date -= dt.timedelta(days=1)
        data_available, _ = has_data(symbol, cur_date, interval=interval, exchange=exchange)
    return cur_date

@cache
def get_data(symbol, date, interval, exchange):
    file_path = KiteUtil.get_file_path(symbol, date, exchange=exchange, interval=interval)
    _, df = has_data(symbol, date, interval, exchange)
    return df

@cache
def find_closest_expiry(symbol, date):
    closest_expiry = None
    min_diff = float('inf')
    for expiry in expiries:
        expiry_dt = icharts.convert_str_to_date(expiry)
        cur_diff = (expiry_dt - date).days
        if cur_diff >= 0 and cur_diff < min_diff:
            min_diff = cur_diff
            closest_expiry = expiry_dt
    return closest_expiry

def get_option_chain_file_path(symbol, expiry, date):
    return (f"data/options-historical/option-chain/{expiry.year}/"
            f"NIFTY_{icharts.convert_date_to_format(expiry)}__"
            f"{icharts.convert_cur_date_to_format(date)}_OptionChain.csv")

@cache
def get_eod_option_chain(symbol, date):
    file_path = KiteUtil.get_file_path(symbol, date, exchange=exchange, interval=interval)
    _, df = has_data(symbol, date, interval, exchange)
    return df
