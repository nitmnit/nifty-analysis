import datetime as dt
import requests
import heapq
import os

import pandas as pd
from bokeh.io import output_notebook, show, curdoc
from bokeh.palettes import Category10
from bokeh.plotting import figure
from bokeh.models import (
    ColumnDataSource,
    BooleanFilter,
    CDSView,
    IndexFilter,
    CrosshairTool,
    LabelSet,
    HoverTool,
    WheelZoomTool,
    DatetimeTickFormatter,
    DatetimeTicker,
    TickFormatter,
    CustomJSTickFormatter,
    Label,
    CustomJS,
    NumeralTickFormatter,
)
from bokeh.layouts import gridplot
from bokeh.themes import Theme
import icharts as ic
from base import OrderManager
from constants import *
from historical_data import KiteUtil
from icharts_config import expiries
from logger_settings import logger


# output_notebook()
file_path = "api-scrip-master.csv"
scrip_df = pd.read_csv(file_path)


def convert_float(num):
    num = round(num, 2)  # Ensure the number has 2 decimal places
    integer_part = int(num)
    decimal_part = int(10 * (num - integer_part))
    last_digit = int(10 * (10 * (num - integer_part) - decimal_part))
    # If the last digit is less than 5, make it 0. Otherwise, make it 5.
    if last_digit < 5:
        last_digit = 0
    else:
        last_digit = 5
    # Construct the new number
    new_num = integer_part + decimal_part / 10 + last_digit / 100
    return round(new_num, 2)


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
            f"time taken by {__name__}: {round((t2-t1).total_seconds(), 2)} seconds"
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
            f"file not found symbol: {symbol} on date: {candle_timestamp}, file_path: {file_path}"
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


def get_date_range(start_date, end_date, symbol, interval, exchange):
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
    return train_dates, test_dates


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
            # (index_field, f"@{index_field}"),
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


def is_file_old(file_path, days=7):
    if os.path.exists(file_path):
        modified_time = dt.datetime.fromtimestamp(os.path.getmtime(file_path))
        return dt.datetime.now() - modified_time >= dt.timedelta(days=days)
    return False


def download_and_replace(url, file_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, "wb") as file:
            file.write(response.content)
        return True
    else:
        print("Failed to download file from URL:", url)
        return False


def get_fo_instrument_details(symbol, expiry, strike, option_type, exchange):
    global scrip_df
    if is_file_old(file_path):
        download_and_replace(
            url="https://images.dhan.co/api-data/api-scrip-master.csv",
            file_path=file_path,
        )
        scrip_df = pd.read_csv(file_path)
    otype = CE if option_type == OPTION_TYPE_CALL else PE
    scrip_df["SEM_EXPIRY_DATE"] = pd.to_datetime(scrip_df["SEM_EXPIRY_DATE"])
    match = scrip_df.loc[
        (scrip_df.SEM_EXPIRY_DATE.dt.date == expiry)
        & (scrip_df.SEM_STRIKE_PRICE == strike)
        & (scrip_df.SEM_OPTION_TYPE == otype)
        & (scrip_df.SEM_TRADING_SYMBOL.str.startswith(symbol))
        & (scrip_df.SEM_EXM_EXCH_ID == exchange)
    ]
    if match.shape[0] > 1:
        raise Exception(f"more than one match found {scrip_df}")
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


def next_thursday():
    today = dt.date.today()
    # Calculate days until next Thursday (Thursday is weekday 3)
    days_ahead = (3 - today.weekday() + 7) % 7
    next_thursday_date = today + dt.timedelta(days=days_ahead)
    return next_thursday_date


class ZerodhaOrderManager(OrderManager):
    def __init__(self):
        pass


def create_candlestick_plot(df):
    cds = ColumnDataSource(df)
    green = CDSView(filter=BooleanFilter(df["close"] >= df["open"]))
    red = CDSView(filter=BooleanFilter(df["close"] < df["open"]))
    w = 60 * 1000
    p = figure(
        x_axis_type="datetime",
        title=f"Minute Candles",
        min_width=2000,
        min_height=900,
        background_fill_color="#1e1e1e",
    )

    # Segments for high-low
    p.segment(
        x0="date_time",
        y0="high",
        x1="date_time",
        y1="low",
        color="#26a69a",
        source=cds,
        view=green,
    )
    p.segment(
        x0="date_time",
        y0="high",
        x1="date_time",
        y1="low",
        color="#ef5350",
        source=cds,
        view=red,
    )

    # Bars for open-close
    p.vbar(
        x="date_time",
        width=w,
        top="open",
        bottom="close",
        fill_color="#26a69a",
        line_color="black",
        source=cds,
        view=green,
    )
    p.vbar(
        x="date_time",
        width=w,
        top="close",
        bottom="open",
        fill_color="#ef5350",
        line_color="black",
        source=cds,
        view=red,
    )

    p.xaxis.formatter = CustomJSTickFormatter(
        code="""
        var date = new Date(tick);
        var hours = date.getUTCHours();
        var minutes = date.getUTCMinutes();
        var suffix = (hours >= 12) ? 'PM' : 'AM';
        hours = (hours % 12) || 12;
        minutes = minutes < 10 ? '0' + minutes : minutes;
        return hours + ':' + minutes;
    """
    )

    p.xaxis.ticker = DatetimeTicker(desired_num_ticks=30, num_minor_ticks=5)

    p.grid.grid_line_alpha = 0.3

    crosshair_tool = CrosshairTool(
        dimensions="both",
        line_color="red",
        line_alpha=0.8,
    )
    p.add_tools(crosshair_tool)
    hover = HoverTool(
        tooltips=[
            ("Date", "@date_time{%H:%M}"),
            ("Open", "@open"),
            ("High", "@high"),
            ("Low", "@low"),
            ("Close", "@close"),
            ("Volume", "@volume{0.0a}"),
        ],
        formatters={"@date_time": "datetime"},
        mode="vline",
    )
    p.add_tools(hover)
    wheel_zoom = WheelZoomTool()
    p.add_tools(wheel_zoom)
    p.toolbar.active_scroll = wheel_zoom

    tradingview_theme = Theme(
        json={
            "attrs": {
                "figure": {
                    "background_fill_color": "#1e1e1e",
                    "border_fill_color": "#1e1e1e",
                    "outline_line_color": "#393939",
                },
                "Axis": {
                    "major_label_text_color": "#e0e0e0",
                    "axis_label_text_color": "#e0e0e0",
                    "major_tick_line_color": "#393939",
                    "minor_tick_line_color": "#393939",
                    "axis_line_color": "#393939",
                },
                "Grid": {"grid_line_color": "#393939"},
            }
        }
    )

    # Create labels for crosshair values on the axes
    x_label = Label(
        x=0,
        y=0,
        x_units="data",
        y_units="screen",
        text="",
        text_color="white",
        text_font_size="10pt",
        background_fill_color="#1e1e1e",
        background_fill_alpha=0.8,
        text_align="left",
        text_baseline="bottom",
    )

    y_label = Label(
        x=0,
        y=0,
        x_units="screen",
        y_units="data",
        text="",
        text_color="white",
        text_font_size="10pt",
        background_fill_color="#1e1e1e",
        background_fill_alpha=0.8,
        text_align="left",
        text_baseline="bottom",
    )

    p.add_layout(x_label, "below")
    p.add_layout(y_label, "below")

    # CustomJS callback to update labels
    callback = CustomJS(
        args={"x_label": x_label, "y_label": y_label, "plot": p},
        code="""
        const { x, y } = cb_data['geometry'];
        const { sx, sy } = cb_data['geometry'];
        const plotHeight = plot.height;
        if (sx !== undefined || sy !== undefined) {
            const date = new Date(x);
            var hours = date.getUTCHours();
            var minutes = date.getUTCMinutes();
            hours = (hours % 12) || 12;
            minutes = minutes < 10 ? '0' + minutes : minutes;
            const xval = hours + ':' + minutes;
    
            const yValue = y.toFixed(2);
    
            x_label.x =  x;
            x_label.y = 0;  // Slightly offset from the bottom
            x_label.text = xval;
    
            y_label.x = 0;  // Slightly offset from the left
            y_label.y = y;
            y_label.text = yValue;
    
            x_label.visible = true;
            y_label.visible = true;
        }
    """,
    )

    # Add hover tool to update labels
    # nhover = HoverTool(tooltips=None)

    hover.callback = callback

    # p.add_tools(nhover)

    curdoc().theme = tradingview_theme

    volume_fig = figure(
        x_axis_type="datetime",
        title="Volume",
        min_width=2000,
        height=250,  # Adjust height as needed
        background_fill_color="#1e1e1e",
        x_range=p.x_range,
    )

    # Bars for volume
    volume_fig.vbar(
        x="date_time",
        width=w,
        top="volume",
        fill_color="#26a69a",
        line_color="black",
        source=cds,
        view=green,
    )
    volume_fig.vbar(
        x="date_time",
        width=w,
        top="volume",
        fill_color="#ef5350",
        line_color="black",
        source=cds,
        view=red,
    )

    volume_fig.xaxis.formatter = CustomJSTickFormatter(
        code="""
        var date = new Date(tick);
        var hours = date.getUTCHours();
        var minutes = date.getUTCMinutes();
        var suffix = (hours >= 12) ? 'PM' : 'AM';
        hours = (hours % 12) || 12;
        minutes = minutes < 10 ? '0' + minutes : minutes;
        return hours + ':' + minutes;
    """
    )

    volume_fig.xaxis.ticker = DatetimeTicker(desired_num_ticks=30, num_minor_ticks=5)

    volume_fig.yaxis.formatter = NumeralTickFormatter(format="0.0a")

    volume_fig.grid.grid_line_alpha = 0.3

    volume_fig.add_tools(crosshair_tool)
    volume_fig.add_tools(hover)
    volume_fig.add_tools(wheel_zoom)
    volume_fig.toolbar.active_scroll = wheel_zoom

    layout = gridplot([[p], [volume_fig]])
    show(layout)
    return layout

def get_strike_price_by_price(symbol, expiry, timestamp, option_type, price, exchange):
    open_price = get_price_at(symbol=symbol, d=timestamp.date(), t=timestamp.time(), interval=INTERVAL_MIN1, exchange=exchange, get_open=False)
    atm_strike = get_atm_strike(open_price)
    premium = get_premium_at(symbol=symbol, expiry=expiry, strike_price=atm_strike, date=timestamp.date(), option_type=option_type, tm=timestamp.time(), get_open=False)
    cur_diff = premium - price
    increment = 50
    if (cur_diff > 0) != (option_type == OPTION_TYPE_CALL):
        increment = -50
    cur_diff = 1000000
    prev_diff = cur_diff + 1
    prev_strike = atm_strike
    cur_strike = atm_strike + increment
    while abs(cur_diff) < abs(prev_diff):
        premium = get_premium_at(symbol=symbol, expiry=expiry, strike_price=cur_strike, date=timestamp.date(), option_type=option_type, tm=timestamp.time(), get_open=False)
        prev_diff = cur_diff
        cur_diff = premium - price
        cur_strike += increment
    return cur_strike - increment

