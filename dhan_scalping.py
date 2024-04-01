import dhan_base as dhb
import os
import math
import datetime as dt
import config
from dhanhq import dhanhq
import utils as ut

from constants import EXCHANGE_NFO, OPTION_TYPE_CALL, OPTION_TYPE_PUT, EXCHANGE_NSE


MODE_CALL = OPTION_TYPE_CALL
MODE_PUT = OPTION_TYPE_PUT

dhan = dhanhq(config.DHAN_CLIENT_ID, config.DHAN_ACCESS_TOKEN)
dhan.get_fund_limits()


def next_thursday():
    today = dt.date.today()
    # Calculate days until next Thursday (Thursday is weekday 3)
    days_ahead = (3 - today.weekday() + 7) % 7
    next_thursday_date = today + dt.timedelta(days=days_ahead)
    return next_thursday_date


class Config:
    def __init__(self):
        self.call_strike = int(os.getenv("CALL_STRIKE"))
        self.put_strike = int(os.getenv("PUT_STRIKE"))
        self.mode = os.getenv("CUR_MODE")
        self.call_quantity = int(os.getenv("CALL_QTY"))
        self.put_quantity = int(os.getenv("PUT_QTY"))
        self.call_tp = float(os.getenv("CALL_TP"))
        self.put_tp = float(os.getenv("PUT_TP"))
        self.symbol = "NIFTY"
        self.exchange = EXCHANGE_NFO
        self.expiry = next_thursday()
        self.icall = None  # Call instrument
        self.iput = None
        self.call_order = None
        self.put_order = None
        self.set_call_instrument()
        self.set_put_instrument()

    def set_call_instrument(self):
        self.icall = ut.get_fo_instrument_details(
            symbol=self.symbol,
            expiry=self.expiry,
            strike=self.call_strike,
            option_type=OPTION_TYPE_CALL,
            exchange=EXCHANGE_NSE,
        )
        print(self.icall)

    def set_put_instrument(self):
        self.iput = ut.get_fo_instrument_details(
            symbol=self.symbol,
            expiry=self.expiry,
            strike=self.put_strike,
            option_type=OPTION_TYPE_PUT,
            exchange=EXCHANGE_NSE,
        )
        print(self.iput)

    def buy_call(self):
        ltp = dhb.get_instrument_ltp(self.icall)
        od = self.get_order_defaults(
            instrument=self.icall,
            quantity=self.call_quantity,
            expiry=self.expiry,
            option_type=OPTION_TYPE_CALL,
            strike=self.call_strike,
        )
        od["price"] = ltp
        self.call_order = dhan.place_order(**od)
        print(self.call_order)

    def buy_put(self):
        ltp = dhb.get_instrument_ltp(self.icall)
        od = self.get_order_defaults(
            instrument=self.iput,
            quantity=self.put_quantity,
            expiry=self.expiry,
            option_type=OPTION_TYPE_PUT,
            strike=self.put_strike,
        )
        od["price"] = ltp
        self.put_order = dhan.place_order(**od)
        print(self.put_order)

    def square_off_call(self):
        ltp = dhb.get_instrument_ltp(self.icall)
        od = self.get_order_defaults(
            instrument=self.icall,
            quantity=self.call_quantity,
            expiry=self.expiry,
            option_type=OPTION_TYPE_CALL,
            strike=self.call_strike,
        )
        od["price"] = ltp
        od["transaction_type"] = dhan.SELL
        order = dhan.place_order(**od)
        self.put_order = None
        print(order)

    def square_off_put(self):
        ltp = dhb.get_instrument_ltp(self.iput)
        od = self.get_order_defaults(
            instrument=self.iput,
            quantity=self.put_quantity,
            expiry=self.expiry,
            option_type=OPTION_TYPE_PUT,
            strike=self.put_strike,
        )
        od["price"] = ltp
        od["transaction_type"] = dhan.SELL
        order = dhan.place_order(**od)
        self.call_order = None
        print(order)

    def square_off_all(self):
        self.square_off_call()
        self.square_off_put()

    def get_order_defaults(self, instrument, quantity, expiry, option_type, strike):
        order_details = {
            "security_id": instrument["SEM_SMST_SECURITY_ID"],
            "exchange_segment": dhan.NSE_FNO,
            "transaction_type": dhan.BUY,
            "quantity": quantity,
            "order_type": dhan.LIMIT,
            "product_type": dhan.MARGIN,
            "price": None,
            "disclosed_quantity": math.ceil(quantity * 0.31),
            "validity": dhan.DAY,
            "drv_expiry_date": expiry.strftime("%Y-%m-%d"),
            "drv_options_type": option_type,
            "drv_strike_price": strike,
            "tag": "h4-vol",
        }
        return order_details


config = Config()

while True:
    full_command = input()
    splitted = full_command.split(" ")
    command = splitted[0]
    args = None
    if len(splitted) > 1:
        args = splitted[1:]

    if command == "f":  # Buy
        if config.mode == MODE_CALL:
            config.buy_call()
        elif config.mode == MODE_PUT:
            config.buy_put()
        else:
            print("invalid mode set to make a buy")
    elif command == "fc":  # Buy call
        config.buy_call()
    elif command == "fp":  # Buy put
        config.buy_put()
    elif command == "fa":  # Buy both call and put
        config.buy_call()
        config.buy_put()
    elif command == "s":  # Squareoff
        if config.mode == MODE_CALL:
            config.square_off_call()
        elif config.mode == MODE_PUT:
            config.square_off_put()
        else:
            print("invalid mode to square off")
    elif command == "sa":  # Squareoff All
        config.square_off_call()
        config.square_off_put()
    elif command.startswith("m"):
        mode = command[1]

        if mode == "c":
            config.mode = MODE_CALL
        elif mode == "p":
            config.mode = MODE_PUT
        else:
            print(f"error: invalid mode, choose either {
                  MODE_CALL} or {MODE_PUT}")
            continue
        print(f"changed mode: {config.mode}")
    elif command in ["strike", "st"]:
        if config.mode == MODE_CALL:
            config.call_strike = int(args[0])
            config.set_call_instrument()
            print(f"changed call strike: {config.call_strike}")
        elif config.mode == MODE_PUT:
            config.put_strike = int(args[0])
            config.set_put_instrument()
            print(f"changed put strike: {config.put_strike}")
        else:
            print("error: first set the trading mode")
    elif command in ["quantity", "q"]:
        if config.mode == MODE_CALL:
            config.call_quantity = int(args[0])
            print(f"changed call quantity: {config.call_quantity}")
        elif config.mode == MODE_PUT:
            config.put_quantity = int(args[0])
            print(f"changed put quantity: {config.put_quantity}")
        else:
            print("error: first set the trading mode")
    elif command in ["tp", "t"]:
        if config.mode == MODE_CALL:
            config.call_tp = float(args[0])
            print(f"changed call tp: {config.call_tp}")
        elif config.mode == MODE_PUT:
            config.put_tp = float(args[0])
            print(f"changed put tp: {config.put_tp}")
        else:
            print("error: first set the trading mode")
    elif command in ["exit", "qq"]:
        print("Exiting!")
        exit("Exiting")
    else:
        print(f"command not found {command}")
