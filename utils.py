import datetime as dt
import heapq
import os

import pandas as pd
from bokeh.io import output_notebook, show
from bokeh.models import (
    CrosshairTool,
    HoverTool,
    LabelSet,
    WheelZoomTool,
    ColumnDataSource,
)
from bokeh.palettes import Category10
from bokeh.plotting import figure

import icharts as ic
from base import OrderManager
from constants import *
from historical_data import KiteUtil
from icharts_config import expiries
from logger_settings import logger


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
        logger.info(
            f"time taken by {__name__}: {
                round((t2-t1).total_seconds(), 2)} seconds"
        )
        return res

    return wraps


def get_date(timestamp):
    if isinstance(timestamp, dt.datetime):
        return timestamp.date()
    elif isinstance(timestamp, dt.date):
        return timestamp
    else:
        raise Exception(f"not date {type(timestamp)}")


def has_data(symbol, candle_timestamp, interval, exchange):
    file_path = KiteUtil.get_file_path(
        symbol, candle_timestamp, exchange=exchange, interval=interval
    )
    try:
        df = pd.read_csv(file_path, index_col="date", parse_dates=True)
        if interval == INTERVAL_DAY:
            df = df.loc[df.index.date == get_date(candle_timestamp)]
    except FileNotFoundError:
        logger.info(
            f"file not found symbol: {symbol} on date: {
                candle_timestamp}, file_path: {file_path}"
        )
        return False, None
    except pd.errors.EmptyDataError:
        return False, None
    return df.shape[0] != 0, df


def get_last_trading_day(symbol, date, interval, exchange):
    cur_date = date
    data_available = False
    while not data_available:
        cur_date -= dt.timedelta(days=1)
        data_available, _ = has_data(
            symbol, cur_date, interval=interval, exchange=exchange
        )
    return cur_date


def get_data(symbol, date, interval, exchange):
    if isinstance(date, dt.datetime):
        date = date.date()
    file_path = KiteUtil.get_file_path(
        symbol, date, exchange=exchange, interval=interval
    )
    _, df = has_data(symbol, date, interval, exchange)
    return df.loc[df.index.date == date]


def get_data_interval(symbol, from_date, to_date, interval, exchange):
    if interval == INTERVAL_DAY:
        raise Exception("use get_data for Day interval")
    if isinstance(from_date, dt.datetime):
        from_date = from_date.date()
    if isinstance(to_date, dt.datetime):
        to_date = to_date.date()
    results = []
    cur_date = from_date
    while cur_date <= to_date:
        file_path = KiteUtil.get_file_path(
            symbol, cur_date, exchange=exchange, interval=interval
        )
        _, df = has_data(symbol, cur_date, interval, exchange)
        results += df.to_dict(orient="records", index=True)
        cur_date += dt.timedelta(days=1)
    df = pd.DataFrame(results)
    return df


def find_closest_expiry(symbol, date):
    closest_expiry = None
    min_diff = float("inf")
    for expiry in expiries:
        expiry_dt = ic.convert_str_to_date(expiry)
        cur_diff = (expiry_dt - date).days
        if cur_diff >= 0 and cur_diff < min_diff:
            min_diff = cur_diff
            closest_expiry = expiry_dt
    return closest_expiry


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
    return (
        f"data/options-historical/option-chain/{expiry.year}/"
        f"NIFTY_{ic.convert_date_to_format(expiry)}__"
        f"{ic.convert_cur_date_to_format(date)}_OptionChain.csv"
    )


def get_eod_option_chain(symbol, date):
    file_path = KiteUtil.get_file_path(
        symbol, date, exchange=exchange, interval=interval
    )
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
    all_dates = pd.DataFrame(
        {
            "trade_date": build_date_range(
                start_date, end_date, symbol, interval, exchange
            )
        }
    )
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
        ocdf = ocdf.loc[
            ((row.market_open - 700) < ocdf.index)
            & ((row.market_open + 700) > ocdf.index)
        ]
        ocdf.loc[:, "trade_date"] = row.name
        for col, val in row.items():
            ocdf[col] = val
        result.append(ocdf)
    ocdf = pd.concat(result)


def get_quantity(buy_price, lot_size, investment):
    return (investment // (buy_price * lot_size)) * lot_size


def bokeh_plot(
    cds,
    x_label,
    y_label,
    freq=None,
    plot="circle",
    multi_plots=None,
    subplots=None,
    subplot_labels=None,
    index_field="id",
):
    output_notebook()
    p = figure(
        title="Bokeh Line Plot",
        x_axis_label=x_label,
        y_axis_label=y_label,
        min_width=2100,
        min_height=1000,
    )
    p.xaxis.ticker.desired_num_ticks = 40  # Tick every 5 minutes

    crosshair_tool = CrosshairTool(
        dimensions="both",
        line_color="red",
        line_alpha=0.8,
    )
    p.add_tools(crosshair_tool)
    p.toolbar.active_scroll = p.select_one(WheelZoomTool)
    if plot == "line":
        line1 = p.line(
            x=index_field,
            y="last_price",
            source=cds,
            line_width=2,
            line_color="green",
            line_alpha=0.5,
        )
    elif plot == "circle":
        p.circle(x=index_field, y="last_price", source=cds, line_width=2)
    hvt = HoverTool(
        tooltips=[
            ("last_trade_time", "@last_trade_time{%H:%M:%S}"),
            (
                "last_price",
                "$@{last_price}{%0.2f}",
            ),  # use @{ } for field names with spaces
            ("volume", "@volume{0.00 a}"),
            ("oi", "@oi{0.00000 a}"),
            (index_field, f"@{index_field}"),
            ("index", "@index"),
            ("(x,y)", "($x{int}, $y)"),
        ],
        formatters={
            "@last_trade_time": "datetime",  # use 'datetime' formatter for '@date' field
            # use 'printf' formatter for '@{adj close}' field
            "@{last_price}": "printf",
        },
        # display a tooltip whenever the cursor is vertically in line with a glyph
        mode="vline",
        renderers=[line1],
    )
    p.add_tools(hvt)
    if multi_plots:
        draw_sub_multiline_plot(p, cds=multi_plots)
    if subplots:
        draw_sub_plot(p, subplots, subplot_labels)
    show(p)


def draw_sub_plot(p, subplots, subplot_labels):
    for i, subplot in enumerate(subplots):
        p.triangle(
            x="x",
            y="y",
            source=subplot,
            size=10,
            color=Category10[10][i],
            legend_label=subplot_labels[i],
        )


def draw_sub_multiline_plot(p, cds):
    p.multi_line(xs="xs", ys="ys", line_width=2, color="orange", source=cds)
    label_set = LabelSet(x="x", y="y", text="texts", x_offset=5, y_offset=5, source=cds)
    p.add_layout(label_set)


def get_price_at(symbol, d, t, interval, exchange, get_open=True):
    data = get_data(symbol=symbol, date=d, interval=interval, exchange=exchange)
    try:
        if get_open:
            return data.loc[data.index.time == t].iloc[0].open
        else:
            return data.loc[data.index.time == t].iloc[0].close
    except IndexError:
        return pd.NA
    except AttributeError:
        return pd.NA


def get_fo_instrument_details(symbol, expiry, strike, option_type, exchange):
    df = pd.read_csv("api-scrip-master.csv")
    otype = CE if option_type == OPTION_TYPE_CALL else PE
    df["SEM_EXPIRY_DATE"] = pd.to_datetime(df["SEM_EXPIRY_DATE"])
    match = df.loc[
        (df.SEM_EXPIRY_DATE.dt.date == expiry)
        & (df.SEM_STRIKE_PRICE == strike)
        & (df.SEM_OPTION_TYPE == otype)
        & (df.SEM_TRADING_SYMBOL.str.startswith(symbol))
        & (df.SEM_EXM_EXCH_ID == exchange)
    ]
    if match.shape[0] > 1:
        raise Exception(f"more than one match found {df}")
    if match.empty:
        return pd.NA
    return match.iloc[0].to_dict()


def add_to_time(time, minutes):
    return (
        dt.datetime.combine(dt.datetime.now(), time) + dt.timedelta(minutes=minutes)
    ).time()


def get_premium_at(symbol, expiry, strike_price, date, option_type, tm, get_open=True):
    pr = ic.get_opt_pre_df(
        symbol=symbol,
        expiry=expiry,
        cur_dt=date,
        strike_price=strike_price,
        option_type=option_type,
    )
    if type(pr) == type(pd.NA) or pr.shape[0] == 0:
        return pd.NA
    candle = pr.loc[(pr.index.date == date) & (pr.index.time == tm)]
    if candle.shape[0] == 0:
        return pd.NA
    if get_open:
        return candle.iloc[0].open
    else:
        return candle.iloc[0].close


def get_atm_strike(price):
    return round(price / 50) * 50


def get_tick_file_path(symbol, expiry, strike, type, date):
    return os.path.join(
        "data/ticks/", f"{symbol}-{expiry}-{int(strike)}-{type}-{date}.csv"
    )


def get_ticks(symbol, expiry, strike, otype, date):
    ticks_columns = {
        "last_price": float,
        "last_traded_quantity": int,
        "total_buy_quantity": int,
        "total_sell_quantity": int,
        "volume_traded": "int",
        "oi": int,
    }
    file_path = get_tick_file_path(
        symbol=symbol, expiry=expiry, strike=strike, type=otype, date=date
    )
    tdf = pd.read_csv(file_path, dtype=ticks_columns)
    columns_to_drop = set(tdf.columns).difference(ticks_columns.keys())
    columns_to_drop.remove("last_trade_time")
    tdf.drop(columns=columns_to_drop, inplace=True)
    tdf["last_trade_time"] = pd.to_datetime(
        tdf["last_trade_time"], format="%Y-%m-%d %H:%M:%S"
    )
    tdf["volume"] = tdf.volume_traded - tdf.volume_traded.shift(1)
    tdf.drop("volume_traded", inplace=True, axis=1)
    tdf.fillna({"volume": 0}, inplace=True)
    tdf["id"] = (tdf.last_trade_time - tdf.iloc[0].last_trade_time).dt.total_seconds()
    tdf["id"] = pd.to_numeric(tdf["id"], downcast="integer")
    return tdf


def bokeh_series_plot(df, y_name, x_name):
    output_notebook()
    source = ColumnDataSource(df)
    p = figure(
        title="Pandas Series Line Graph",
        x_axis_label=x_name,
        y_axis_label=y_name,
        min_width=2100,
        min_height=1000,
    )
    p.line(
        x=x_name, y=y_name, source=source, line_width=2
    )  # Adjust line_width as needed
    crosshair_tool = CrosshairTool(
        dimensions="both",
        line_color="red",
        line_alpha=0.8,
    )
    p.add_tools(crosshair_tool)
    p.toolbar.active_scroll = p.select_one(WheelZoomTool)
    p.xaxis.ticker.desired_num_ticks = 40  # Tick every 5 minutes
    hvt = HoverTool(
        tooltips=[
            (y_name, f"@{y_name}"),
            (x_name, f"@{x_name}"),
            ("index", "@index"),
            ("(x,y)", "($x{int}, $y)"),
        ],
        formatters={
            "@last_trade_time": "datetime",  # use 'datetime' formatter for '@date' field
            # use 'printf' formatter for '@{adj close}' field
            "@{last_price}": "printf",
        },
        # display a tooltip whenever the cursor is vertically in line with a glyph
        mode="vline",
    )
    p.add_tools(hvt)
    show(p)


class ZerodhaOrderManager(OrderManager):
    def __init__(self):
        pass
