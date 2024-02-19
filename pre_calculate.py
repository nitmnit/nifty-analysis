import os
import datetime as dt
import icharts
import historical_data as hd
import pandas as pd
import json
from collections import defaultdict
from constants import *
from logger_settings import logger
import time


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

def ct(fn):
    def wraps(*args, **kwargs):
        t1 = dt.datetime.now()
        fn(*args, **kwargs)
        t2 = dt.datetime.now()
        logger.info(f"time taken: fn: {fn.__name__} seconds: {round((t2-t1).total_seconds(), 5)}")
    return wraps

"""
Prepare premiums of interest
Fetch previous day close
Consider all possibilities
Put everything in pickle dataframe to be loaded
"""
 
############  CONSTANTS #############
IS_LIVE = True
INJECT = False
MINIMUM_PREMIUM = 90
MIN_VOLUME = 2 * 10 ** 9 # 1B
MAX_OC = 10
IC_SYMBOL = "NIFTY"
KITE_SYMBOL = "NIFTY 50"
EXPIRY = (dt.datetime.strptime("2024-02-22", "%Y-%m-%d")).date()
#EXPIRY = (dt.datetime.strptime("2024-01-18", "%Y-%m-%d")).date()
TODAY = dt.datetime.now().date()
#TODAY = dt.datetime(year=2024, month=1, day=18).date()
MARKET_OPEN = dt.time(hour=9, minute=15)
WINDOW_CLOSE = dt.time(hour=15, minute=15, second=10)
if IS_LIVE is False:
    #TODAY = TODAY - dt.timedelta(days=2)
    pass
PREVIOUS_TRADING_DAY = TODAY - dt.timedelta(days=1)
TODAY_OCDF_PICKLE_FILE_NAME = f"prev_day_oc_analysis_trade_date_{TODAY}.pkl"
NIFTY_LOWER_SIDE = 500 # Points down from previous close
NIFTY_UPPER_SIDE = 500 # Points down from previous close
MIN_GAP = 40 # Gap from previous close
PREMIUM_THRESHOLD_PC = .10 # Premium might open this down at max to be considered, .3 is 30%
BO_LT = .015 # Bracket order limit price w.r.t. actual open price
BO_TP = .03 # Target profit percentage w.r.t. buying price
BO_SL = .03 # SL percentage w.r.t. buying price
BUY_QUANTITY = 50 # Number of lots as used by dhan
#BUY_QUANTITY = 1 # Number of lots as used by dhan
############  CONSTANTS END #############


def get_previous_day_close():
    global PREVIOUS_TRADING_DAY
    ku = hd.KiteUtil(exchange=EXCHANGE_NSE)
    prev_day_candle = False
    while not prev_day_candle:
        prev_day_candle = ku.fetch_stock_data(symbol=KITE_SYMBOL, from_date=PREVIOUS_TRADING_DAY, to_date=PREVIOUS_TRADING_DAY, interval=hd.INTERVAL_DAY)
        if not prev_day_candle:
            logger.info(f"Changing previous trading day: {PREVIOUS_TRADING_DAY}")
            PREVIOUS_TRADING_DAY = PREVIOUS_TRADING_DAY - dt.timedelta(days=1)
    prev_day_close = prev_day_candle[-1]["close"]
    logger.info(f"Found previous day close: {prev_day_close}")
    return prev_day_close


get_previous_day_close()

logger.info(f"""
Configuration
LIVE: {IS_LIVE}
INJECT: {INJECT}
EXPIRY: {EXPIRY}
TODAY: {TODAY}
PREVIOUS TRADING DAY: {PREVIOUS_TRADING_DAY}
NIFTY_LOWER_SIDE: {NIFTY_LOWER_SIDE}
NIFTY_UPPER_SIDE: {NIFTY_UPPER_SIDE}
""")


def prepare_ocdf():
    ku = hd.KiteUtil(exchange=EXCHANGE_NFO)
    try:
        ocdf = icharts.get_oc_df(IC_SYMBOL, EXPIRY, PREVIOUS_TRADING_DAY)
    except FileNotFoundError:
        logger.info("file not found")
        oc = icharts.fetch_option_chain(symbol=IC_SYMBOL, date=PREVIOUS_TRADING_DAY, expiry=EXPIRY)
        print(oc)
        icharts.save_option_chain_to_file(oc=oc, symbol=IC_SYMBOL, expiry=EXPIRY, date=PREVIOUS_TRADING_DAY)
        ocdf = icharts.get_oc_df(IC_SYMBOL, EXPIRY, PREVIOUS_TRADING_DAY)

    # Filter out option chains which are not of interest
    separated_cp = []
    for i, row in ocdf.iterrows():
        time.sleep(.1)
        ce = {}
        for col, val in row.items():
            if col.startswith("ce_"):
                ce[col[3:]] = val
        ce["option_type"] = OPTION_TYPE_CALL
        ce["expiry"] = row.expiry
        ce["oc_date"] = row.oc_date
        ce["strike_price"] = row.name
        separated_cp.append(ce)
        pe = {}
        for col, val in row.items():
            if col.startswith("pe_"):
                pe[col[3:]] = val
        pe["option_type"] = icharts.OPTION_TYPE_PUT
        pe["expiry"] = row.expiry
        pe["oc_date"] = row.oc_date
        pe["strike_price"] = row.name
        separated_cp.append(pe)

    ocdf = pd.DataFrame(separated_cp)
    # Filter minimum premium
    ocdf = ocdf.loc[ocdf.ltp >= MINIMUM_PREMIUM]

    # Filter minimum volume
    ocdf = ocdf.loc[ocdf.volume >= MIN_VOLUME]
    ocdf.loc[ocdf.option_type==OPTION_TYPE_CALL, "display_ot"] = "CALL"
    ocdf.loc[ocdf.option_type==OPTION_TYPE_PUT, "display_ot"] = "PUT"
    ocdf["instrument_token"] = ocdf.apply(lambda r: ku.get_fo_instrument(IC_SYMBOL, r.expiry, r.strike_price, r.option_type)["instrument_token"], axis=1)
    ocdf["exchange_token"] = ocdf.apply(lambda r: ku.get_fo_instrument(IC_SYMBOL, r.expiry, r.strike_price, r.option_type)["exchange_token"], axis=1)
    ocdf.set_index("instrument_token", inplace=True)
    ocdf["ltp"] = ocdf.apply(lambda r: ku.fetch_stock_data_it(r.name, PREVIOUS_TRADING_DAY, PREVIOUS_TRADING_DAY, INTERVAL_DAY)[0]["close"], axis=1)
    logger.info(ocdf[['time', 'expiry', 'strike_price', 'option_type', 'delta', 'theta', 'ltp', 'exchange_token', "volume"]
    #logger.info(ocdf[['time', 'expiry', 'strike_price', 'option_type', 'delta', 'ltp', 'exchange_token', 'latest', 'change']
])
    #logger.info(ocdf.shape)
    #logger.info(ocdf[ocdf.option_type=="C"])
    #logger.info(ocdf[ocdf.option_type=="C"][["strike_price", "volume", "oi", "oi_chg"]])
    #logger.info(ocdf[ocdf.option_type=="P"])
    ocdf.to_pickle(f"prev_day_oc_analysis_trade_date_{TODAY}.pkl")
    # End filtering OC


def calculate_results():
    results = {}
    tick = cm(0.05)
    counter = 0
    nifty_low = prev_day_close - NIFTY_LOWER_SIDE
    nifty_high = prev_day_close + NIFTY_UPPER_SIDE
    logger.info(f"Nifty Low: {nifty_low}, mid: {prev_day_close}, High: {nifty_high}")
    for nifty_open in range(nifty_low, nifty_high + 1):
        if abs(prev_day_close - nifty_open) < MIN_GAP:
            continue
        results[nifty_open] = {}
        nifty_open_ch = cm(nifty_open - prev_day_close)
        for i, row in ocdf.iterrows(): 
            if nifty_open_ch <= 0 and row.option_type == icharts.OPTION_TYPE_CALL:
                continue
            if nifty_open_ch >= 0 and row.option_type == icharts.OPTION_TYPE_PUT:
                continue
            if row.option_type == icharts.OPTION_TYPE_PUT:
                nifty_open_ch = - nifty_open_ch
            ex_chg_pt = nifty_open_ch * row.delta + row.theta
            ex_chg_pt = ex_chg_pt
            ex_chg_pc = ex_chg_pt / row.ltp
            ex_ltp = row.ltp + ex_chg_pt
            lower_limit = ex_ltp * (1-PREMIUM_THRESHOLD_PC)
            ex_price = lower_limit
            while ex_price < ex_ltp:
                ex_price += tick
                ac_ch_pt = ex_price - row.ltp
                ac_ch_pc = ac_ch_pt / row.ltp
                ac_ex_diff = ac_ch_pc - ex_chg_pc
                if ac_ex_diff < 0:
                    bo = {
                        "lt": cm(ex_price * (1+BO_LT)),
                        "tp": cm(ex_price * (1+BO_LT) * BO_TP),
                        "sl": cm(ex_price * BO_SL),
                    }
                    results[nifty_open][row.strike_price] = {}
                    results[nifty_open][row.strike_price][ex_price] = bo
                counter += 1
        logger.info(f"nifty: {nifty_open}, counter: {counter}")

    logger.info(f"Final counter: {counter}")
    logger.info("writing file")
    df = pd.DataFrame(results)
    df.to_json("precal.json")


def calculate_expected_premium(r, market_open_pt):
    return r.delta * market_open_pt + r.theta

def calculate_today_results(ocdf, nifty_open, prev_day_close):
    results = {}
    counter = 0
    gap_cleared = abs(prev_day_close - nifty_open) >= MIN_GAP
    if not gap_cleared:
        logger.info(f"Gap not enough today. NO TRADE gap: {prev_day_close - nifty_open}, prev: {prev_day_close}, nifty: {nifty_open}")
    nifty_open_ch = nifty_open - prev_day_close
    nifty_change_pt = nifty_open - prev_day_close
    ocdf["ec_pt"] = ocdf.apply(lambda r: calculate_expected_premium(r, nifty_change_pt), axis=1) # ec - expected points change in premium
    ocdf["ec_pc"] = ocdf["ec_pt"] / ocdf["ltp"]
    ocdf["actual_chg_pc"] = ocdf["change"] / ocdf["ltp"]
    ocdf["ac_ex_diff"] = ocdf["actual_chg_pc"] - ocdf["ec_pc"]
    ocdf = ocdf.loc[ocdf.ac_ex_diff < 0]
    ocdf = ocdf.sort_values(by="ac_ex_diff")
    if ocdf.shape[0] > 0:
        return gap_cleared, ocdf.iloc[0]
    else:
        logger.info("no match found")
        return False, None

def modify_ticks_for_testing(ocdf, ticks):
    ku = hd.KiteUtil(exchange=EXCHANGE_NFO)
    if INJECT is not True:
        exit("Inject is False")
    for tick in ticks:
        ltp = ku.fetch_stock_data_it(tick["instrument_token"], TODAY, TODAY, INTERVAL_DAY)[0]["open"]
        tick["last_price"] = ltp
    return ticks
