from bokeh.plotting import figure
from bokeh.io import show, output_notebook
from bokeh.models import CrosshairTool, Range1d
from historical_data import KiteUtil
import pandas as pd
import datetime as dt
import icharts as ic
from icharts_config import expiries
from functools import cache
from constants import *
from pytz import timezone  # For timezone handling
import numpy as np
from logger_settings import logger
import heapq


class MaxHeap:
    def __init__(self, max_size):
        self.max_size = max_size
        self.heap = []

    def push(self, value):
        if len(self.heap) < self.max_size:
            heapq.heappush(self.heap, (-value[0], value))
        else:
            if value[0] > -self.heap[0][0]:
                heapq.heappop(self.heap)
                heapq.heappush(self.heap, (-value[0], value))

    def pop(self):
        return heapq.heappop(self.heap)[1]

    def peek(self):
        return self.heap[0][1]

    def size(self):
        return len(self.heap)


def ct(fn):
    def wraps(*args, **kwargs):
        t1 = dt.datetime.now()
        res = fn(*args, **kwargs)
        t2 = dt.datetime.now()
        logger.info(f"time taken by {__name__}: {round((t2-t1).total_seconds(), 2)} seconds")
        return res
    return wraps

def get_date(timestamp):
    if isinstance(timestamp, dt.datetime):
        return timestamp.date()
    elif isinstance(timestamp, dt.date):
        return timestamp
    else:
        raise Exception(f"not date {type(timestamp)}")
 
@cache
def has_data(symbol, candle_timestamp, interval, exchange):
    file_path = KiteUtil.get_file_path(symbol, candle_timestamp, exchange=exchange, interval=interval)
    try:
        df = pd.read_csv(file_path, index_col="date", parse_dates=True)
        if interval == INTERVAL_DAY:
            df = df.loc[df.index.date == get_date(candle_timestamp)]
    except (pd.errors.EmptyDataError, FileNotFoundError):
        logger.info(f"file not found or empty dateframe for symbol: {symbol} on date: {candle_timestamp}, file_path: {file_path}")
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
    if isinstance(date, dt.datetime):
        date = date.date()
    file_path = KiteUtil.get_file_path(symbol, date, exchange=exchange, interval=interval)
    _, df = has_data(symbol, date, interval, exchange)
    return df.loc[df.index.date == date]

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

@cache
def find_nclosest_expiry(symbol, date, n):
    cur_level = 1
    dexpiries = [ic.convert_str_to_date(expiry) for expiry in expiries]
    dexpiries.sort()
    for expiry in dexpiries:
        cur_diff = (expiry - date).days
        if cur_diff >= 0:
            if cur_level == n:
                return expiry
            cur_level += 1

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

def get_quantity(buy_price, lot_size, investment):
    return (investment // (buy_price * lot_size)) * lot_size

def bokeh_plot(x, y, x_label, y_label, freq=None):
    output_notebook()
    TOOLS = "pan,crosshair,wheel_zoom,hover,box_zoom,reset,save"
    p = figure(title="Bokeh Line Plot", x_axis_label=x_label, y_axis_label=y_label, min_width=1500)
    p.xaxis.ticker.desired_num_ticks = 40  # Tick every 5 minutes
    crosshair_tool = CrosshairTool(
                dimensions="both",
                line_color="red",
                line_alpha=0.8,
            )
    p.add_tools(crosshair_tool)
    p.circle(x=x, y=y, line_width=2)
    show(p)

