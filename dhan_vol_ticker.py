import datetime as dt
import os
from dhanhq import dhanhq, marketfeed
from constants import *
import utils as ut
import config
import dhan_base as dhb


dhan = dhanhq(config.DHAN_CLIENT_ID, config.DHAN_ACCESS_TOKEN)
dhan.get_fund_limits()
atm_strike = int(os.getenv("CALL_STRIKE"))

strikes = list(range(atm_strike - 200, atm_strike + 200, 50))

expiry = ut.next_thursday()
expiry = expiry - dt.timedelta(days=1)

instruments = [
    ut.get_fo_instrument_details(
        symbol="NIFTY",
        expiry=expiry,
        strike=strike,
        option_type=OPTION_TYPE_CALL,
        exchange=EXCHANGE_NSE,
    )
    for strike in strikes
]

instruments += [
    ut.get_fo_instrument_details(
        symbol="NIFTY",
        expiry=expiry,
        strike=strike,
        option_type=OPTION_TYPE_PUT,
        exchange=EXCHANGE_NSE,
    )
    for strike in strikes
]

imap = {inst["SEM_SMST_SECURITY_ID"]: inst for inst in instruments}


async def on_connect(instance):
    print("Connected to websocket")


async def on_message(instance, message):
    print(message)
    if "LTP" in message:
        dhb.set_instrument_ltp(imap[message["security_id"]], message["LTP"])


subscription_code = marketfeed.Ticker
insts = [
    (DHAN_EXCHANGE_SEGMENTS_ENUM["NSE_FNO"], str(inst["SEM_SMST_SECURITY_ID"]))
    for inst in instruments
]
print(insts)
feed = marketfeed.DhanFeed(
    config.DHAN_CLIENT_ID,
    config.DHAN_ACCESS_TOKEN,
    insts,
    subscription_code,
    on_connect=on_connect,
    on_message=on_message,
)
feed.run_forever()
