from kiteconnect import KiteTicker
import datetime as dt
from logger_settings import logger
import config
import historical_data as hd
from constants import *
import sys
import json

IS_LIVE = sys.argv[1] == "live"
IC_SYMBOL = "NIFTY"
KITE_SYMBOL = "NIFTY 50"
EXPIRY = (dt.datetime.strptime(sys.argv[2], "%Y-%m-%d")).date()
TODAY = (dt.datetime.strptime(sys.argv[3], "%Y-%m-%d")).date()
STRIKE = int(sys.argv[4])
OPTION_TYPE = sys.argv[5]
assert OPTION_TYPE in [OPTION_TYPE_CALL, OPTION_TYPE_PUT]

# Initialise
ku = hd.KiteUtil(exchange=EXCHANGE_NFO)
kws = KiteTicker(config.KITE_API_KEY, ku.access_token)
instrument = ku.get_fo_instrument(IC_SYMBOL, EXPIRY, STRIKE, OPTION_TYPE)
print(instrument)


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, dt.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


def on_ticks(ws, ticks):
    # Callback to receive ticks.
    logger.info(f"{json.dumps(ticks, cls=DateTimeEncoder)}")
    if ws._vol_in_order:
        pass

def on_connect(ws, response):
    ws.subscribe([instrument["instrument_token"]])
    ws.set_mode(ws.MODE_FULL, [instrument["instrument_token"]])
    ws._vol_in_order = False

def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()

def on_order_update(ws, data):
    logger.info(f"order update received: {data}")

# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
#kws.on_close = on_close
kws.on_order_update = on_order_update

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect()
