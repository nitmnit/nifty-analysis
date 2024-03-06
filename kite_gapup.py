import datetime as dt
import asyncio
import math
from dhanhq import dhanhq, marketfeed
import historical_data as hd
from constants import *
from kiteconnect import KiteTicker
from logger_settings import logger
import sys
import utils as ut
import time
import config


logger.info("Start dhan_gapup")
NIFTY_IT = 256265

def get_strike(strike, i):
    divider = 50
    reminder = strike % divider
    if reminder != 0:
        return divider * (strike // divider) - i * divider
    return divider * (strike // divider) - (i+1) * divider


@ut.ct
def get_order_details(prs):
    atm = round(prs["nifty_before_buy"] / 50) * 50
    selected_strike = get_strike(atm, prs["strike_i"])
    instrument = ut.get_fo_instrument_details(prs["symbol"], expiry=prs["expiry"], strike=selected_strike, option_type=OPTION_TYPE_CALL, exchange=EXCHANGE_NSE)
    logger.info(selected_strike)
    logger.info(f"instrument details: {instrument}")
    order_details = {
        "security_id": instrument["SEM_SMST_SECURITY_ID"],
        "exchange_segment": dhanhq.NSE_FNO,
        "transaction_type": dhanhq.BUY,
        "quantity": prs["quantity"],
        "order_type": dhanhq.LIMIT,
        "product_type": dhanhq.MARGIN,
        "price": None,
        "disclosed_quantity": math.ceil(prs["quantity"] * .31),
        "validity": dhanhq.DAY,
        "drv_expiry_date": prs["expiry"].strftime("%Y-%m-%d"),
        "drv_options_type": "CALL" if prs["option_type"] == OPTION_TYPE_CALL else 'PUT',
        "drv_strike_price": selected_strike,
        "tag": 'h1',
    }
    return order_details


def on_ticks(ws, ticks):
    logger.info("Ticks: {}".format(ticks))
    if dt.datetime.now().time() < ws.GAP_PRS["nifty_price_time"]:
        return
    if ws.GAP_PRS["is_first"]:
        for tick in ticks:
            if tick["instrument_token"] == NIFTY_IT:
                ws.GAP_PRS["nifty_before_buy"] = tick["last_price"]
                ws.GAP_PRS["is_first"] = False
                ws.GAP_OD = get_order_details(ws.GAP_PRS)
                ws.GAP_IT = ws.ku.get_fo_instrument(ws.GAP_PRS["symbol"], ws.GAP_PRS["expiry"], ws.GAP_OD["drv_strike_price"], ws.GAP_PRS["option_type"])["instrument_token"]
                on_connect(ws, None)
    elif dt.datetime.now().time() >= ws.GAP_PRS["buy_at"]:
        for tick in ticks:
            if tick["instrument_token"] == ws.GAP_IT:
                ws.GAP_OD["price"] = tick["last_price"]
                if ws.GAP_PRS["is_live"] and False:
                    order = ws.dhan.place_order(**(ws.GAP_OD))
                    logger.info("order: {order}")
                logger.info(f"Placed {ws.GAP_PRS['is_live']}, order details: {ws.GAP_OD}")
                ws.stop()

def on_connect(ws, response):
    if ws.GAP_PRS["is_first"]:
        ws.subscribe([NIFTY_IT])
        ws.set_mode(ws.MODE_LTP, [NIFTY_IT])
    else:
        logger.info(f"Subscribing to option {ws.GAP_IT}")
        ws.subscribe([ws.GAP_IT])
        ws.unsubscribe([NIFTY_IT])
        ws.set_mode(ws.MODE_LTP, [ws.GAP_IT])


def on_close(ws, code, reason):
    logger.info("websocket stopped")
    ws.stop()

def subscribe(prs):
    logger.info(f"settings: {prs}")
    ku = hd.KiteUtil(exchange=EXCHANGE_NFO)
    kws = KiteTicker(config.KITE_API_KEY, ku.access_token)
    kws.ku = ku
    kws.GAP_PRS = prs
    # Assign the callbacks.
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.dhan = dhanhq(config.DHAN_CLIENT_ID, config.DHAN_ACCESS_TOKEN)
    kws.dhan.get_fund_limits()

    # Infinite loop on the main thread. Nothing after this will run.
    # You have to use the pre-defined callbacks to manage subscriptions.
    kws.connect()


if __name__ == '__main__':
    is_live = sys.argv[1] == "live"
    command = sys.argv[2]



    if command == 'entry':
        waiting = True
        time.sleep(1)
        buy_date = dt.datetime.strptime(sys.argv[3], "%Y-%m-%d").date()
        sell_date = dt.datetime.strptime(sys.argv[4], "%Y-%m-%d").date()
        expiry = dt.datetime.strptime(sys.argv[5], "%Y-%m-%d").date()
        nifty_price_time = dt.datetime.strptime(sys.argv[6], "%H:%M").time()
        buy_at = dt.datetime.strptime(sys.argv[7], "%H:%M").time()

        while is_live and (dt.datetime.now().time() < (dt.datetime.combine(dt.datetime.today(), nifty_price_time) - dt.timedelta(seconds=20)).time()):
            if waiting:
                logger.info(f"waiting, curtime: {dt.datetime.now()}")
                waiting = False 
        if not waiting:
            logger.info("wait over")
        prs = {
                "nifty_price_time": nifty_price_time,
                "buy_at": buy_at,
                "kite_symbol": "NIFTY 50",
                "symbol": "NIFTY",
                "interval": INTERVAL_MIN1,
                "exchange": EXCHANGE_NFO,
                "strike_i": -5,
                "is_live": is_live,
                "buy_date": buy_date,
                "sell_date": sell_date,
                "expiry": expiry,
                "quantity": 10,
                "option_type": OPTION_TYPE_CALL,
                "exchange_segment_enum": DHAN_EXCHANGE_SEGMENTS_ENUM["NSE_FNO"],
                "is_entry": True, 
                "is_first": True,
        }
        logger.info(prs)
        subscribe(prs)
    elif command == 'exit':
        gapup_exit()

