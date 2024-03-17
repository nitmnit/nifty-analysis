from kiteconnect import KiteTicker
import datetime as dt
from tick_logger_settings import logger
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
# STRIKE = int(sys.argv[4])
ATM = int(sys.argv[4])
STRIKES = list(range(ATM - 500, ATM + 500, 50))
# STRIKES = list(map(int, sys.argv[4].split(',')))
# OPTION_TYPE = sys.argv[5]
# assert OPTION_TYPE in [OPTION_TYPE_CALL, OPTION_TYPE_PUT]

# Initialise
ku = hd.KiteUtil(exchange=EXCHANGE_NFO)
kws = KiteTicker(config.KITE_API_KEY, ku.access_token)

instruments = [
    ku.get_fo_instrument(IC_SYMBOL, EXPIRY, STRIKE, OPTION_TYPE_CALL)
    for STRIKE in STRIKES
]
instruments += [
    ku.get_fo_instrument(IC_SYMBOL, EXPIRY, STRIKE, OPTION_TYPE_PUT)
    for STRIKE in STRIKES
]
NIFTY_ITOKEN = ku.get_nse_instrument_token("NIFTY 50")
print(instruments)

logger.info("[")


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, dt.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


def on_ticks(ws, ticks):
    # Callback to receive ticks.
    json_str = json.dumps(ticks, cls=DateTimeEncoder)
    json_str[0] = " "
    json_str[-1] = " "
    logger.info(f"{json.dumps(ticks, cls=DateTimeEncoder)}")
    if ws._vol_in_order:
        pass


def on_connect(ws, response):
    itokens = [instrument["instrument_token"] for instrument in instruments]
    itokens.append(NIFTY_ITOKEN)
    ws.subscribe(itokens)
    ws.set_mode(ws.MODE_FULL, itokens)
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
# kws.on_close = on_close
kws.on_order_update = on_order_update

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
try:
    kws.connect()
except KeyboardInterrupt as e:
    logger.info("]")
    raise e
