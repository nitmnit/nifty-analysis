import os
import sys
import datetime as dt
import icharts
import historical_data as hd
import pandas as pd
import json
from collections import defaultdict
from constants import *
from logger_settings import logger
import time


logger.info("Start trading")
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


ku = hd.KiteUtil(exchange=EXCHANGE_NFO)

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
        res = fn(*args, **kwargs)
        t2 = dt.datetime.now()
        logger.info(f"time taken: fn: {fn.__name__} seconds: {round((t2-t1).total_seconds(), 5)}")
        return res
    return wraps

"""
Prepare premiums of interest
Fetch previous day close
Consider all possibilities
Put everything in pickle dataframe to be loaded
"""
 
############  CONSTANTS #############
IS_LIVE = sys.argv[1] == "live"
MINIMUM_PREMIUM = 90
MIN_VOLUME = 2 * (10 ** 7) # 1B
MAX_OC = 10
IC_SYMBOL = "NIFTY"
KITE_SYMBOL = "NIFTY 50"
EXPIRY = (dt.datetime.strptime(sys.argv[2], "%Y-%m-%d")).date()
TODAY = (dt.datetime.strptime(sys.argv[3], "%Y-%m-%d")).date()
MARKET_OPEN = dt.time(hour=9, minute=15)
WINDOW_CLOSE = dt.time(hour=9, minute=15, second=10)
PRE_MARKET_CLOSE = dt.time(hour=9, minute=8, second=10) # Adding extra 10 second to avoid any time differences
PREVIOUS_TRADING_DAY = TODAY - dt.timedelta(days=1)
TODAY_OCDF_PICKLE_FILE_NAME = f"prev_day_oc_analysis_trade_date_{TODAY}.pkl"
NIFTY_LOWER_SIDE = 500 # Points down from previous close
NIFTY_UPPER_SIDE = 500 # Points down from previous close
MIN_GAP = 20 # Gap from previous close
PREMIUM_THRESHOLD_PC = .10 # Premium might open this down at max to be considered, .3 is 30%
BO_LT = .015 # Bracket order limit price w.r.t. actual open price
EC_PT_RIDE = .89 # How much expectation you are willing to ride
BO_TP = .055 # Target profit percentage w.r.t. buying price
#BO_SL = .015 # SL percentage w.r.t. buying price
BO_SL = .10 # SL percentage w.r.t. buying price
MIN_SL = .021
BUY_QUANTITY = 50 # Number of lots as used by dhan
NIFTY_ITOKEN = ku.get_nse_instrument_token("NIFTY 50")
############  CONSTANTS END #############


def get_previous_day_close():
    global PREVIOUS_TRADING_DAY
    prev_day_candle = False
    x = hd.KiteUtil(exchange=EXCHANGE_NSE)
    while not prev_day_candle:
        prev_day_candle = x.fetch_stock_data(symbol=KITE_SYMBOL, from_date=PREVIOUS_TRADING_DAY, to_date=PREVIOUS_TRADING_DAY, interval=hd.INTERVAL_DAY)
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
EXPIRY: {EXPIRY}
TODAY: {TODAY}
PREVIOUS TRADING DAY: {PREVIOUS_TRADING_DAY}
""")


def prepare_ocdf():
    try:
        ocdf = icharts.get_oc_df(IC_SYMBOL, EXPIRY, PREVIOUS_TRADING_DAY)
    except FileNotFoundError:
        logger.info("file not found")
        oc = icharts.fetch_option_chain(symbol=IC_SYMBOL, date=PREVIOUS_TRADING_DAY, expiry=EXPIRY)
        icharts.save_option_chain_to_file(oc=oc, symbol=IC_SYMBOL, expiry=EXPIRY, date=PREVIOUS_TRADING_DAY)
        ocdf = icharts.get_oc_df(IC_SYMBOL, EXPIRY, PREVIOUS_TRADING_DAY)
    # Filter out option chains which are not of interest
    separated_cp = []
    for i, row in ocdf.iterrows():
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
    #ocdf["ltp"] = ocdf.apply(lambda r: ku.fetch_stock_data_it(r.name, PREVIOUS_TRADING_DAY, PREVIOUS_TRADING_DAY, INTERVAL_DAY)[0]["close"], axis=1)
    logger.info(ocdf[['time', 'expiry', 'strike_price', 'option_type', 'delta', 'theta', 'ltp', 'exchange_token', "volume"]
    #logger.info(ocdf[['time', 'expiry', 'strike_price', 'option_type', 'delta', 'ltp', 'exchange_token', 'latest', 'change']
])
    #logger.info(ocdf.shape)
    #logger.info(ocdf[ocdf.option_type=="C"])
    #logger.info(ocdf[ocdf.option_type=="C"][["strike_price", "volume", "oi", "oi_chg"]])
    #logger.info(ocdf[ocdf.option_type=="P"])
    ocdf.to_pickle(f"prev_day_oc_analysis_trade_date_{TODAY}.pkl")
    # End filtering OC


def calculate_expected_premium(r, market_open_pt):
    return r.delta * market_open_pt + r.theta

@ct
def calculate_today_results(ocdf, nifty_open, prev_day_close):
    results = {}
    counter = 0
    gap_cleared = abs(prev_day_close - nifty_open) >= MIN_GAP
    if not gap_cleared:
        logger.info(f"Gap not enough today. NO TRADE gap: {prev_day_close - nifty_open}, prev: {prev_day_close}, nifty: {nifty_open}")
    nifty_change_pt = nifty_open - prev_day_close
    selected_option_type = OPTION_TYPE_CALL if nifty_change_pt >= 0 else OPTION_TYPE_PUT
    ocdf.drop((ocdf.loc[ocdf.option_type != selected_option_type].index), inplace=True)
    ocdf["ec_pt"] = ocdf.delta * nifty_change_pt + ocdf.theta
    if selected_option_type == OPTION_TYPE_PUT:
        ocdf["ec_pt"] = - ocdf["ec_pt"]
    ocdf["ac_ec_pt_diff"] = ocdf["ec_pt"] - ocdf["change"]
    ocdf["ec_pc"] = ocdf["ec_pt"] / ocdf["ltp"]
    ocdf["actual_chg_pc"] = ocdf["change"] / ocdf["ltp"]
    ocdf["ac_ex_diff"] = ocdf["actual_chg_pc"] - ocdf["ec_pc"]
    if IS_LIVE is False:
        logger.info(ocdf[['time', 'expiry', 'strike_price', 'option_type', 'delta', 'ltp', 'volume', 'exchange_token', 'latest', 'change', 'ec_pt', 'ec_pc', 'actual_chg_pc', 'ac_ex_diff']])
    filtered = ocdf.drop(ocdf.loc[(ocdf.ec_pc <= 0) | (ocdf.ac_ex_diff > 0) | ocdf.ac_ex_diff.isna()].index)
    filtered.sort_values(by="ac_ex_diff", inplace=True)
    if filtered.shape[0] > 0:
        return gap_cleared, filtered.iloc[0]
    else:
        logger.info("no match found")
        return False, None

def modify_ticks_for_testing(ocdf, ticks):
    if IS_LIVE is not False:
        exit("IS_LIVE is True")
    new_ticks = []
    for i, row in ocdf.iterrows():
        tick = ticks[0].copy()
        tick["instrument_token"] = row.name
        tick["last_price"] = ku.fetch_stock_data_it(tick["instrument_token"], TODAY, TODAY, INTERVAL_DAY)[0]["open"]
        new_ticks.append(tick)
    return new_ticks


def modify_ticks_for_testing_ift(ocdf, ticks):
    if IS_LIVE is not False:
        exit("IS_LIVE is True")
    new_ticks = []
    for i, row in ocdf.iterrows():
        tick = ticks[0].copy()
        tick["instrument_token"] = row.name
        tick["last_price"] = ku.fetch_stock_data_it(tick["instrument_token"], PREVIOUS_TRADING_DAY, PREVIOUS_TRADING_DAY, INTERVAL_DAY)[0]["close"]
        new_ticks.append(tick)
    tick = ticks[0].copy()
    tick["instrument_token"] = NIFTY_ITOKEN
    tick["last_price"] = ku.fetch_stock_data_it(tick["instrument_token"], TODAY, TODAY, INTERVAL_DAY)[0]["open"]
    new_ticks.append(tick)
    return new_ticks

def filter_ocdf_on_nifty_open(nifty_open, prev_close, ocdf):
    """
    If opens up, remove puts, otherwise remove calls 
    """
    is_up = (nifty_open - prev_close) > 0
    option_type = OPTION_TYPE_CALL if is_up else OPTION_TYPE_PUT
    ocdf = ocdf.loc[(ocdf.option_type == option_type)]
    if option_type == OPTION_TYPE_CALL:
        ocdf = ocdf.loc[(ocdf.strike_price < nifty_open) & (ocdf.strike_price > prev_close - 300)]
    else:
        ocdf = ocdf.loc[(ocdf.strike_price > nifty_open) & (ocdf.strike_price < prev_close + 300)]
    return ocdf

