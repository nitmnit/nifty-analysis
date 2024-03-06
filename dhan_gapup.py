import datetime as dt
import asyncio
import math
from dhanhq import dhanhq, marketfeed
from constants import *
from logger_settings import logger
import sys
import utils as ut
import time
import config


logger.info("Start dhan_gapup")

dhan = dhanhq(config.DHAN_CLIENT_ID, config.DHAN_ACCESS_TOKEN)
dhan.get_fund_limits()

if __name__ == '__main__':
    is_live = sys.argv[1] == "live"
    command = sys.argv[2]
    buy_date = dtstrptime(sys.argv[3], "%Y-%m-%d").date()
    sell_date = dtstrptime(sys.argv[4], "%Y-%m-%d").date()
    expiry = dtstrptime(sys.argv[5], "%Y-%m-%d").date()

    prs = {
            "nifty_price_time": dt.time(hour=15, minute=20),
            "buy_at": dt.time(hour=15, minute=26),
            "kite_symbol": "NIFTY 50",
            "symbol": "NIFTY",
            "interval": INTERVAL_MINUTE,
            "exchange": EXCHANGE_NSE,
            "strike_i": -5,
            "is_live": is_live,
            "buy_date": buy_date,
            "sell_date": sell_date,
            "expiry": expiry,
            "quantity": 10,
            "option_type": "CALL",
            "exchange_segment_enum": DHAN_EXCHANGE_SEGMENTS_ENUM["NSE_FNO"],
    }

    logger.info(prs)
    waiting = True

    while is_live and (dt.dtnow() < prs["nifty_price_time"] + dt.timedelta(minute=2)):
        if waiting:
            logger.info(f"waiting, curtime: {dt.dtnow()}")
            waiting = False
        time.sleep(1)

    if not waiting:
        logger.info("wait over")

    if command == 'entry':
        od = get_order_details(prs)
        subscribe(od)
    elif command == 'exit':
        gapup_exit()

def get_strike(strike, i):
    divider = 50
    reminder = strike % divider
    if reminder != 0:
        return divider * (strike // divider) - i * divider
    return divider * (strike // divider) - (i+1) * divider


@ut.ct
def get_order_details(prs):
    nifty_before_buy = ut.get_price_at(symbol=prs["kite_symbol"], d=prs["buy_date"], t=prs["nifty_price_time"], interval=prs["interval"], exchange=prs["exchange"])
    atm = round(nifty_before_buy / 50) * 50
    selected_strike = get_strike(atm, prs["strike_i"])
    instrument = ut.get_fo_instrument_details(prs["symbol"], expiry=prs["expiry"], strike=selected_strike, option_type=OPTION_TYPE_CALL, exchange=prs["exchange"])
    order_details = {
        "security_id": instrument["SEM_SMST_SECURITY_ID"],
        "exchange_segment": dhan.NSE_FNO,
        "transaction_type": dhan.BUY,
        "quantity": prs["quantity"],
        "order_type": dhan.LIMIT,
        "product_type": dhan.MARGIN,
        "price": None,
        "disclosed_quantity": math.ceil(prs["quantity"] * .31),
        "validity": dhan.DAY,
        "drv_expiry_date": prs["expiry"].strftime("%Y-%m-%d"),
        "drv_options_type": prs["option_type"],
        "drv_strike_price": selected_strike,
        "tag": 'h1',
    }
    return order_details


async def on_connect(instance):
    logger.info("Connected to websocket")

async def on_message(instance, message):
    logger.info(f"Received: {message}")

    if instance.GAP_ORDER_PRS["is_live"]:
        instance.GAP_ORDER_DETAILS["price"] = message["LTP"]
        order = dhan.place_order(**instance.GAP_ORDER_DETAILS)
        logger.info(f"Placed {instance.GAP_ORDER_PRS['is_live']} order: {order}")


def subscribe(prs, od):
    subscription_code = marketfeed.Ticker
    logger.info(f"order details: {od}")
    logger.info(f"settigns: {prs}")
    instruments = [(prs["exchange_segment_enum"], str(od["security_id"]))]
    feed = marketfeed.DhanFeed(
        config.DHAN_CLIENT_ID,
        config.DHAN_ACCESS_TOKEN,
        instruments,
        subscription_code,
        on_connect=on_connect,
        on_message=on_message)
    feed.GAP_ORDER_DETAILS = od
    feed.GAP_ORDER_PRS = prs
    #loop = asyncio.get_event_loop()
    #loop.run_until_complete(feed.run_forever())
    #loop = asyncio.get_event_loop()
    feed.run_forever()
    asyncio.create_task(feed.run_forever())

