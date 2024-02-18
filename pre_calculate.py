import os
import datetime as dt
import icharts
import historical_data as hd
import pandas as pd
import json
from decimal import Decimal, ROUND_UP
from collections import defaultdict
from constants import *



def cm(money):
    return Decimal(str(money)).quantize(Decimal('0.05'), rounding=ROUND_UP)

"""
Prepare premiums of interest
Fetch previous day close
Consider all possibilities
Put everything in pickle dataframe to be loaded
"""

############  CONSTANTS #############
IS_LIVE = False
MINIMUM_PREMIUM = 90
MIN_VOLUME = 300000000
IC_SYMBOL = "NIFTY"
KITE_SYMBOL = "NIFTY 50"
EXPIRY = (dt.datetime.strptime("2024-02-22", "%Y-%m-%d")).date()
TODAY = (dt.datetime.now() - dt.timedelta(days=2)).date()
PREVIOUS_TRADING_DAY = TODAY - dt.timedelta(days=1)
PREVIOUS_DAY_CLOSE_FILE_NAME = f"PREV_DAY_CL_{PREVIOUS_TRADING_DAY}"
TODAY_OCDF_PICKLE_FILE_NAME = f"prev_day_oc_analysis_trade_date_{TODAY}.pkl"
NIFTY_LOWER_SIDE = 100 # Points down from previous close
NIFTY_UPPER_SIDE = 100 # Points down from previous close
MIN_GAP = 40 # Gap from previous close
PREMIUM_THRESHOLD_PC = .10 # Premium might open this down at max to be considered, .3 is 30%
BO_LT = .01 # Bracket order limit price w.r.t. actual open price
BO_TP = .01 # Target profit percentage w.r.t. buying price
BO_SL = .01 # SL percentage w.r.t. buying price
BUY_QUANTITY = 1 # Number of lots as used by dhan
############  CONSTANTS END #############
print(f"""
Configuration
LIVE: {IS_LIVE}
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
        print("file not found")
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
        instrument = ku.get_fo_instrument(IC_SYMBOL, row.expiry, row.name, OPTION_TYPE_CALL)
        ce["instrument_token"] = instrument["instrument_token"]
        ce["exchange_token"] = instrument["exchange_token"]
        separated_cp.append(ce)
        pe = {}
        for col, val in row.items():
            if col.startswith("pe_"):
                pe[col[3:]] = val
        pe["option_type"] = icharts.OPTION_TYPE_PUT
        pe["expiry"] = row.expiry
        pe["oc_date"] = row.oc_date
        pe["strike_price"] = row.name
        instrument = ku.get_fo_instrument(IC_SYMBOL, row.expiry, row.name, OPTION_TYPE_PUT)
        pe["instrument_token"] = instrument["instrument_token"]
        pe["exchange_token"] = instrument["exchange_token"]
        separated_cp.append(pe)

    ocdf = pd.DataFrame(separated_cp)
    ocdf.set_index("instrument_token", inplace=True)

    # Filter minimum premium
    ocdf = ocdf.loc[ocdf.ltp >= MINIMUM_PREMIUM]

    # Filter minimum volume
    ocdf = ocdf.loc[ocdf.volume >= MIN_VOLUME]
    ocdf[ocdf.option_type==OPTION_TYPE_CALL]["display_ot"] = "CALL"
    ocdf[ocdf.option_type==OPTION_TYPE_PUT]["display_ot"] = "PUT"
    print(ocdf.shape)
    print(ocdf[ocdf.option_type=="C"])
    print(ocdf[ocdf.option_type=="C"][["strike_price", "volume", "oi", "oi_chg"]])
    print(ocdf[ocdf.option_type=="P"])
    ocdf.to_pickle(f"prev_day_oc_analysis_trade_date_{TODAY}.pkl")
    # End filtering OC

def get_previous_day_close():
    ku = hd.KiteUtil(exchange=EXCHANGE_NSE)
    if os.path.exists(PREVIOUS_DAY_CLOSE_FILE_NAME):
        with open(PREVIOUS_DAY_CLOSE_FILE_NAME, "r") as f:
            prev_day_close = cm(f.read())
    else:
        print("prev day close not found")
        prev_day_candle = ku.fetch_stock_data(symbol=KITE_SYMBOL, from_date=PREVIOUS_TRADING_DAY, to_date=TODAY, interval=hd.INTERVAL_DAY)[0]
        prev_day_close = prev_day_candle["close"]
        with open(PREVIOUS_DAY_CLOSE_FILE_NAME, "w+") as f:
            f.write(f"{prev_day_close}")


def calculate_results():
    results = {}
    tick = cm(0.05)
    counter = 0
    nifty_low = prev_day_close - NIFTY_LOWER_SIDE
    nifty_high = prev_day_close + NIFTY_UPPER_SIDE
    print(f"Nifty Low: {nifty_low}, mid: {prev_day_close}, High: {nifty_high}")
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
            ex_chg_pt = nifty_open_ch * Decimal(row.delta) + Decimal(row.theta)
            ex_chg_pt = cm(ex_chg_pt)
            ex_chg_pc = ex_chg_pt / row.ltp
            ex_ltp = row.ltp + ex_chg_pt
            lower_limit = cm(ex_ltp * (1-Decimal(PREMIUM_THRESHOLD_PC)))
            ex_price = lower_limit
            while ex_price < ex_ltp:
                ex_price += tick
                ac_ch_pt = ex_price - row.ltp
                ac_ch_pc = ac_ch_pt / row.ltp
                ac_ex_diff = ac_ch_pc - ex_chg_pc
                if ac_ex_diff < 0:
                    bo = {
                        "lt": cm(ex_price * (1+Decimal(BO_LT))),
                        "tp": cm(ex_price * (1+Decimal(BO_LT)) * Decimal(BO_TP)),
                        "sl": cm(ex_price * Decimal(BO_SL)),
                    }
                    results[nifty_open][row.strike_price] = {}
                    results[nifty_open][row.strike_price][ex_price] = bo
                counter += 1
        print(f"nifty: {nifty_open}, counter: {counter}")

    print(f"Final counter: {counter}")
    print("writing file")
    df = pd.DataFrame(results)
    df.to_json("precal.json")


def calculate_expected_premium(r, market_open_pt):
    return r.delta * market_open_pt + r.theta

def calculate_today_results(ocdf, nifty_open, prev_day_close):
    results = {}
    tick = cm(0.05)
    counter = 0
    if abs(prev_day_close - nifty_open) < MIN_GAP:
        print("Gap not enough today. NO TRADE")
        return
    nifty_open_ch = cm(nifty_open - prev_day_close)
    nifty_change_pt = nifty_open - prev_day_close
    ocdf["ec_pt"] = ocdf.apply(lambda r: calculate_expected_premium(r, nifty_change_pt), axis=1) # ec - expected points change in premium
    ocdf["ec_pc"] = ocdf["ec_ce_pt"] / ocdf["ltp"]
    ocdf["actual_chg_pc"] = ocdf["change"] / ocdf["ltp"]
    ocdf["ac_ex_diff"] = ocdf["actual_chg_pc"] - ocdf["ec_pc"]
    ocdf = ocdf.loc[ocdf.ac_ex_diff < 0]
    ocdf = ocdf.sort_values(by="ac_ex_diff")
    if ocdf.shape[0] > 0:
        place_order(ocdf.iloc[0])

