import datetime as dt
import dhan_scalping as dsc
import redis
import os
from dhanhq import dhanhq, marketfeed
from constants import *
import utils as ut
import config
import dhan_base as dhb

NO_TRADE = "No Trade"
INITIATE_TRADE = 'Initiate Trade'
IN_TRADE = "In Trade"
TRADE_COOL_DOWN = "Trade Cooldown"
CUR_COOLDOWN = 0
CUR_STATE = NO_TRADE

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

dhan = dhanhq(config.DHAN_CLIENT_ID, config.DHAN_ACCESS_TOKEN)
dhan.get_fund_limits()
atm_strike = int(os.getenv("CALL_STRIKE"))
cfg = dsc.Config()

#strikes = list(range(atm_strike - 200, atm_strike + 200, 50))
# strikes = [os.getenv("CALL_STRIKE")]

expiry = ut.next_thursday()
expiry = expiry - dt.timedelta(days=1)

# instruments = [
#     ut.get_fo_instrument_details(
#         symbol="NIFTY",
#         expiry=expiry,
#         strike=strike,
#         option_type=OPTION_TYPE_CALL,
#         exchange=EXCHANGE_NSE,
#     )
#     for strike in strikes
# ]

# strikes = [os.getenv("PUT_STRIKE")]
# instruments += [
#     ut.get_fo_instrument_details(
#         symbol="NIFTY",
#         expiry=expiry,
#         strike=strike,
#         option_type=OPTION_TYPE_PUT,
#         exchange=EXCHANGE_NSE,
#     )
#     for strike in strikes
# ]

instruments = [cfg.icall, cfg.iput]
imap = {inst["SEM_SMST_SECURITY_ID"]: inst for inst in instruments}


async def on_connect(instance):
    print("Connected to websocket")


async def on_message(instance, message):
    global CUR_STATE, CUR_COOLDOWN
    # if "LTP" in message:
    #     dhb.set_instrument_ltp(imap[message["security_id"]], message["LTP"])
    if message["type"] != "Ticker Data":
        print(message)
        return
    otype = CALL if imap[message["security_id"]]["SEM_OPTION_TYPE"] == "CE" else PUT
    ltp = float(message["LTP"])
    if otype == CALL:
        instance.cticks.append(ltp)
    elif otype == PUT:
        instance.pticks.append(ltp)
    else:
        print(f"None of the call or puts: {message}")
        return
    if CUR_STATE == TRADE_COOL_DOWN:
        positions = cfg.get_open_positions(position_type='long')
        if len(positions) == 0:
            CUR_STATE = NO_TRADE
            cfg.success += 1
            return
        if CUR_COOLDOWN <= 0:
            CUR_STATE = IN_TRADE
        print(f"in cooldown: {CUR_COOLDOWN}")
        CUR_COOLDOWN -= 1
    elif CUR_STATE == NO_TRADE:
        print("inside no trade")
        CUR_STATE = INITIATE_TRADE
        cur_dir = r.get(config.DIRECTION_REDIS_KEY)
        placed = False
        if cur_dir == "up" and otype == CALL:
            # Buy call
            placed = cfg.call_buy_wait_sell(ltp=ltp)
        elif cur_dir == "down" and otype == PUT:
            # Buy put
            placed = cfg.put_buy_wait_sell(ltp=ltp)
        else:
            # Stagnating, nothing to do
            print(f"stagnating {otype}, {cur_dir}, {cur_dir == 'up'}")
            CUR_STATE = NO_TRADE
            return
        if placed:
            CUR_STATE = TRADE_COOL_DOWN
            CUR_COOLDOWN = 25
            instance.cur_orders += 1
        else:
            CUR_STATE = NO_TRADE
    elif CUR_STATE == IN_TRADE:
        # Check if SL is hit or if target is hit, change state to NO_TRADE
        positions = cfg.get_open_positions(position_type='long')
        if len(positions) == 0:
            CUR_STATE = NO_TRADE
            cfg.success += 1
            return
        print(f"in trade")
        if otype == CALL and positions[0]["drvOptionType"] == PUT:
            return
        elif otype == PUT and positions[0]["drvOptionType"] == CALL:
            return
        if otype == CALL:
            r.set(config.DIRECTION_REDIS_KEY, "stag")
            # if len(instance.cticks) >= 60 and min(instance.cticks[-60:]) >= ltp:
            # Trigger stop loss
            if ltp <= cfg.call_od["price"]:
                cfg.square_off_call(ltp=ltp)
            # r.set(config.DIRECTION_REDIS_KEY, "down")
        elif otype == PUT:
            r.set(config.DIRECTION_REDIS_KEY, "stag")
            # if len(instance.pticks) >= 60 and min(instance.pticks[-60:]) >= ltp:
            # Trigger stop loss
            if ltp <= cfg.put_od["price"]:
                cfg.square_off_put(ltp=ltp)
            # r.set(config.DIRECTION_REDIS_KEY, "up")
    else:
        print(f"No conditions match: {CUR_STATE}")


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
feed.cticks = []
feed.pticks = []
feed.cur_orders = 0
feed.run_forever()
