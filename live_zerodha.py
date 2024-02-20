import pre_calculate as pc
import math
from historical_data import KiteUtil
import json
import config
import constants as c
import pandas as pd
import datetime as dt
from dhanhq import dhanhq
from logger_settings import logger


pc.prepare_ocdf()

ocdf = pd.read_pickle(pc.TODAY_OCDF_PICKLE_FILE_NAME)

ocdf["latest"] = pd.NA
ocdf["change"] = 0
# What do I want to do with this?
"""
1. Pre calculate all the data required from previous day option chain
2. Create -300 to +300 NIFTY 50 opening and expected changes in the premium of strike prices of interest
3. Create combinations of opening prices for all premiums and order ENTRY and EXIT details.
3. Subscribe to live data from Zerodha and Dhan. See their speeds. And subscribe to all the instruments of interest
4. Receive live data in fastest way possible for all premiums of interest and make the order
5. Log all the timings so that I can improve my timings
6. Do I need to move to golang or Rust?
"""

from kiteconnect import KiteTicker

x = KiteUtil(exchange=c.EXCHANGE_NFO)
NIFTY_ITOKEN = x.get_nse_instrument_token("NIFTY 50")
NIFTY_OPEN_TODAY = False
ORDERED = False
EXECUTED = False

if pc.IS_LIVE is False:
    #NIFTY_OPEN_TODAY = float(22020)
    pass
NIFTY_PREV_CLOSE = pc.get_previous_day_close()
assert NIFTY_PREV_CLOSE is not None

# Initialise
kws = KiteTicker(config.KITE_API_KEY, x.access_token)

# Dhan
DHAN_CLIENT_ID = ""
DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzEwODQyNzAwLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMjI2MTY1MiJ9.exTj4lRdMCuqCt0FtY4Y9r0SqcR41Lj8jIPaTOaIl2ZZ6ABMTa18UTGa9HBYPZ9EOU9xjt8Ud_xQSwxTnCI1AQ"
dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
dhan.get_fund_limits()
tokens = [256265]
tokens += ocdf.index.to_list()

@pc.ct
def place_order(ocdf_frame):
    global ORDERED
    if ORDERED:
        return
    ORDERED = True
    #lp = pc.convert_float(ocdf_frame.latest * (1+pc.BO_LT))
    exp = ocdf_frame.ec_pt * .75
    lp = pc.convert_float(ocdf_frame.ltp + (exp * (1 - pc.EC_PT_RIDE)))
    tp = ocdf_frame.ltp + exp - ocdf_frame.latest
    tp = pc.convert_float(tp / 2) # Setting half the expectations
    #tp = pc.convert_float(lp * pc.BO_TP)
    #sl = pc.convert_float(ocdf_frame.latest * pc.BO_SL)

    order_details = {
        "security_id": ocdf_frame.exchange_token, # Done
        "exchange_segment": dhan.NSE_FNO, # Done
        "transaction_type": dhan.BUY, # Done
        "quantity": pc.BUY_QUANTITY, # ???
        "order_type": dhan.LIMIT, # Done
        "product_type": dhan.BO, # Done
        "price": lp,
        "disclosed_quantity": math.ceil(pc.BUY_QUANTITY * .31),
        "validity": dhan.DAY, # Done
        "bo_profit_value": tp,
        "bo_stop_loss_Value": tp,
        "drv_expiry_date": ocdf_frame.expiry.strftime("%Y-%m-%d"),
        "drv_options_type": ocdf_frame.display_ot,
        "drv_strike_price": float(ocdf_frame.strike_price),
        "tag": 'solo', # Done
    }
    if pc.IS_LIVE and pc.INJECT is False:
        order = dhan.place_order(**order_details)
        logger.info(f"Placed live order: {order}")
    logger.info(f"ocdf frame: {ocdf_frame}")
    logger.info(f"IS_LIVE: {pc.IS_LIVE}, INJECT: {pc.INJECT}: Placed order\n {order_details}")
 
@pc.ct
def on_ticks(ws, ticks):
    global NIFTY_OPEN_TODAY, EXECUTED
    cur_time = dt.datetime.now().time()
    if not pc.INJECT and (cur_time < pc.PRE_MARKET_CLOSE or cur_time >= pc.WINDOW_CLOSE):
        logger.info(f"market not opened yet or window gone: {cur_time}")
        return
    if EXECUTED:
        return
    if pc.INJECT:
        logger.info(f"modifying ticks before: {ticks}")
        ticks = pc.modify_ticks_for_testing(ocdf, ticks)
        logger.info(f"modifying ticks after: {ticks}")
    for tick in ticks:
        if tick["instrument_token"] == NIFTY_ITOKEN:
            if NIFTY_OPEN_TODAY is False:
                cur_time = dt.datetime.now().time()
                if cur_time > dt.time(hour=9, minute=15):
                    logger.info("================================================")
                    logger.info(f"Found NIFTY_OPEN: {NIFTY_OPEN_TODAY}")
                    logger.info("================================================")
                    NIFTY_OPEN_TODAY = tick["last_price"]
        elif type(ocdf.loc[tick["instrument_token"], "latest"]) == type(pd.NA):
            ocdf.loc[tick["instrument_token"], "latest"] = tick["last_price"]
    ocdf["change"] = ocdf.latest - ocdf.ltp
    oc_shape = ocdf.shape[0]
    cdm = EXECUTED = NIFTY_OPEN_TODAY is not False and (ocdf.loc[ocdf.change != 0.0].shape[0] > 5)
    if cdm:
        clear, ocdf_frame = pc.calculate_today_results(ocdf, NIFTY_OPEN_TODAY, NIFTY_PREV_CLOSE)
        if clear and ocdf_frame is not None and ocdf_frame.shape[0] > 0:
            place_order(ocdf_frame)
            logger.info(ocdf[['time', 'expiry', 'strike_price', 'option_type', 'delta', 'ltp', 'volume', 'exchange_token', 'latest', 'change', 'ec_pt', 'ec_pc', 'actual_chg_pc', 'ac_ex_diff']])
        logger.info("got all the prices")
        logger.info("disconnect now")
        ws.close()
    else:
        logger.info(f"didn't match all os: {ocdf.shape[0]}, rc: {ocdf.loc[ocdf.change != 0.0].shape[0]} {NIFTY_OPEN_TODAY}")
    logger.info("Ticks: {}".format(json.dumps(ticks)))
    if cdm:
        logger.info(ocdf[['time', 'expiry', 'strike_price', 'option_type', 'delta', 'ltp', 'volume', 'exchange_token', 'latest', 'change', 'ec_pt', 'ec_pc', 'actual_chg_pc', 'ac_ex_diff']])
    else:
        logger.info(ocdf[['expiry', 'oc_date', 'strike_price', 'ltp', 'volume', 'option_type', 'exchange_token', 'latest', 'change']])

def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe(tokens)

    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_LTP, tokens)
    logger.info("connected")

def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    logger.info(f"closed connection: {reason}")
    ws.stop()

# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect()
