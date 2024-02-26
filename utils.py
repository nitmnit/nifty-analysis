from historical_data import KiteUtil
import pandas as pd
import datetime as dt
import icharts as ic
from icharts_config import expiries
from functools import cache
from constants import *
from pytz import timezone  # For timezone handling
import numpy as np


def ct(fn):
    def wraps(*args, **kwargs):
        t1 = dt.datetime.now()
        res = fn(*args, **kwargs)
        t2 = dt.datetime.now()
        print(f"time taken by {__name__}: {round((t2-t1).total_seconds(), 2)} seconds")
        return res
    return wraps

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
        expiry_dt = ic.convert_str_to_date(expiry)
        cur_diff = (expiry_dt - date).days
        if cur_diff >= 0 and cur_diff < min_diff:
            min_diff = cur_diff
            closest_expiry = expiry_dt
    return closest_expiry

def get_option_chain_file_path(symbol, expiry, date):
    return (f"data/options-historical/option-chain/{expiry.year}/"
            f"NIFTY_{ic.convert_date_to_format(expiry)}__"
            f"{ic.convert_cur_date_to_format(date)}_OptionChain.csv")

@cache
def get_eod_option_chain(symbol, date):
    file_path = KiteUtil.get_file_path(symbol, date, exchange=exchange, interval=interval)
    _, df = has_data(symbol, date, interval, exchange)
    return df

def build_date_range(date_start, date_end, symbol, interval, exchange):
    date_range = []
    cur_date = date_start
    while cur_date < date_end:
        if cur_date.weekday() not in [5, 6]:
            hd, _ = has_data(symbol, cur_date, interval=interval, exchange=exchange)
            if hd:
                date_range.append(cur_date)
        cur_date += dt.timedelta(days=1)
    return date_range

def get_training_dates(start_date, end_date, symbol, interval, exchange):
    all_dates = pd.DataFrame({"trade_date": build_date_range(start_date, end_date, symbol, interval, exchange)})
    all_dates_shuffled = all_dates.sample(frac=1, random_state=42)
    train_size = int(0.5 * len(all_dates_shuffled))
    train_dates = all_dates_shuffled.iloc[:train_size]
    test_dates = all_dates_shuffled.iloc[train_size:]
    train_dates = train_dates.sort_values(by="trade_date")
    train_dates.set_index("trade_date", inplace=True)
    return train_dates

def get_option_chains(dates, ic_symbol):
    result = []
    for i, row in dates.iterrows():
        ocdf = ic.get_oc_df(ic_symbol, row.expiry, row.previous_trading_day)
        ocdf = ocdf.loc[((row.market_open - 700) < ocdf.index) & ((row.market_open + 700) > ocdf.index)]
        ocdf.loc[:,"trade_date"] = row.name
        for col, val in row.items():
            ocdf[col] = val
        result.append(ocdf)
    ocdf = pd.concat(result)