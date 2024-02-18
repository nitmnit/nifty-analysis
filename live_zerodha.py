import pre_calculate as pc
from historical_data import KiteUtil
import json
import config
import constants as c
import pandas as pd
import datetime as dt
from decimal import Decimal, ROUND_UP
from dhanhq import dhanhq



pc.prepare_ocdf()

def cm(money):
    return Decimal(str(money)).quantize(Decimal('0.05'), rounding=ROUND_UP)

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

import logging
from kiteconnect import KiteTicker

logging.basicConfig(level=logging.DEBUG)

x = KiteUtil(exchange=c.EXCHANGE_NFO)
NIFTY_ITOKEN = x.get_nse_instrument_token("NIFTY 50")

# Initialise
kws = KiteTicker(config.KITE_API_KEY, x.access_token)

# Dhan
DHAN_CLIENT_ID = ""
DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzEwODQyNzAwLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMjI2MTY1MiJ9.exTj4lRdMCuqCt0FtY4Y9r0SqcR41Lj8jIPaTOaIl2ZZ6ABMTa18UTGa9HBYPZ9EOU9xjt8Ud_xQSwxTnCI1AQ"
dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
print(dhan.get_fund_limits())

print(ocdf)
tokens = [256265]
tokens += ocdf.index.to_list()

def place_order(ocdf_frame):
    order_details = {
        "tag": 'solo', # Done
        "transaction_type": dhan.BUY, # Done
        "exchange_segment": dhan.NSE_FNO, # Done
        "product_type": dhan.BO, # Done
        "order_type": dhan.LIMIT, # Done
        "validity": dhan.DAY, # Done
        "security_id": ocdf_frame.exchange_token, # Done
        "quantity": pc.BUY_QUANTITY, # ???
        "disclosed_quantity": math.ceil(pc.BUY_QUANTITY * .31),
        "price": lp,
        "bo_profit_value": tp,
        "bo_stop_loss_Value": sl,
        "drv_expiry_date": ocdf_frame.expiry,
        "drv_options_type": ocdf_frame.display_ot,
        "drv_strike_price": ocdf_frame.name
    }
    print(f"IS_LIVE: {IS_LIVE}: Placed order\n {order_details}")
    if pc.IS_LIVE:
        dhan.place_order(**order_details)

def on_ticks(ws, ticks):
    # Callback to receive ticks.
    logging.info("Ticks: {}".format(json.dumps(ticks)))
    for tick in ticks:
        if tick["instrument_token"] == 9146114:
            ocdf.loc[tick["instrument_token"], "ltp"] = cm(tick["last_price"])
        if type(ocdf.loc[tick["instrument_token", "latest"]) == type(pd.NA):
            ocdf.loc[tick["instrument_token"], "latest"] = cm(tick["last_price"])
        if tick["instrument_token"] == NIFTY_ITOKEN:
            pass
    ocdf["change"] = ocdf.latest - ocdf.ltp
    if ocdf.shape[0] == ocdf.loc[ocdf.change != 0].shape[0]:
        ws.close()
        print("got all the prices")
        print("disconnect now")
        print(ocdf)
    else:
        print("didn't match all")
        print(ocdf)

def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe(tokens)

    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_LTP, tokens)
    print("connected")

def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    print(f"closed connection: {reason}")
    ws.stop()

# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect()
