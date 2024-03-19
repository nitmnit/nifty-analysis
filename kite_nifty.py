from kiteconnect import KiteTicker
import datetime as dt
from nifty_tick_logger_settings import logger
import config
import historical_data as hd
from constants import *
import json

IC_SYMBOL = "NIFTY"
KITE_SYMBOL = "NIFTY 50"

# Initialise
ku = hd.KiteUtil(exchange=EXCHANGE_NSE)
kws = KiteTicker(config.KITE_API_KEY, ku.access_token)

instruments = [ku.get_nse_instrument_token(
    symbol=symbol) for symbol in NIFTY50_SYMBOLS]
NIFTY_ITOKEN = ku.get_nse_instrument_token(KITE_SYMBOL)
instruments.append(NIFTY_ITOKEN)
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
    json_str = json_str[1:-1]
    logger.info(f"{json.dumps(ticks, cls=DateTimeEncoder)}")
    if ws._vol_in_order:
        pass


def on_connect(ws, response):
    ws.subscribe(instruments)
    ws.set_mode(ws.MODE_FULL, instruments)
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
