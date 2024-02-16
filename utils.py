from historical_data import KiteUtil
import pandas as pd
import datetime as dt
import icharts
from icharts_config import expiries

INTERVAL_MIN1 = "minute"
INTERVAL_MIN3 = "3minute"
INTERVAL_MIN5 = "5minute"
INTERVAL_MIN10 = "10minute"
INTERVAL_MIN15 = "15minute"
INTERVAL_MIN30 = "30minute"
INTERVAL_MIN60 = "60minute"
INTERVAL_DAY = "day"

MAX_PERIOD = {
    INTERVAL_MIN1: 60,
    INTERVAL_MIN3: 100,
    INTERVAL_MIN5: 100,
    INTERVAL_MIN10: 100,
    INTERVAL_MIN15: 200,
    INTERVAL_MIN30: 200,
    INTERVAL_MIN60: 400,
    INTERVAL_DAY: 2000,
}

ALL_INTERVALS = MAX_PERIOD.keys()

EXCHANGE_NSE = "NSE"
ALL_EXCHANGES = [
    EXCHANGE_NSE,
]

def has_data(symbol, candle_timestamp, interval, exchange):
    file_path = KiteUtil.get_file_path(symbol, candle_timestamp, exchange=exchange, interval=interval)
    try:
        df = pd.read_csv(file_path, index_col="date", parse_dates=True)
    except (pd.errors.EmptyDataError, FileNotFoundError):
        print(f"file not found or empty dateframe for symbol: {symbol} on date: {candle_timestamp}, file_path: {file_path}")
        return False, None
    return df.shape[0] != 0, df

def get_last_trading_day(symbol, date, interval, exchange):
    cur_date = date
    data_available = False
    while not data_available:
        cur_date -= dt.timedelta(days=1)
        data_available, _ = has_data(symbol, cur_date, interval=interval, exchange=exchange)
    return cur_date

def get_data(symbol, date, interval, exchange):
    file_path = KiteUtil.get_file_path(symbol, date, exchange=exchange, interval=interval)
    _, df = has_data(symbol, date, interval, exchange)
    return df

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

def get_eod_option_chain(symbol, date):
    file_path = KiteUtil.get_file_path(symbol, date, exchange=exchange, interval=interval)
    _, df = has_data(symbol, date, interval, exchange)
    return df
