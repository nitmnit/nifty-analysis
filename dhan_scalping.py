import dhan_base as dhb
import time
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


class Config:
    def __init__(self):
        self.call_strike = int(os.getenv("CALL_STRIKE"))
        self.put_strike = int(os.getenv("PUT_STRIKE"))
        self.mode = os.getenv("CUR_MODE")
        self.call_quantity = int(os.getenv("CALL_QTY")) * 50
        self.put_quantity = int(os.getenv("PUT_QTY")) * 50
        self.call_tp = float(os.getenv("CALL_TP"))
        self.put_tp = float(os.getenv("PUT_TP"))
        self.symbol = "NIFTY"
        self.exchange = EXCHANGE_NFO
        self.expiry = ut.next_thursday() + dt.timedelta(days=-1)
        self.icall = None  # Call instrument
        self.iput = None
        self.call_order = None
        self.put_order = None
        self.call_od = None
        self.put_od = None
        self.order_timeout = 5  # Timeout in seconds
        self.set_call_instrument()
        self.set_put_instrument()
        self.success = 0
        self.failure = 0

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

    def buy_call(self, ltp):
        od = self.get_order_defaults(
            instrument=self.icall,
            quantity=self.call_quantity,
            expiry=self.expiry,
            option_type=OPTION_TYPE_CALL,
            strike=self.call_strike,
        )
        od["price"] = ltp
        res = dhan.place_order(**od)
        if res["status"] == "success":
            self.call_order = res["data"]
            print(self.call_order)
            self.call_od = od
            return True
        else:
            print(f"failed to place order: {res}, od: {od}")
            return False

    def buy_put(self, ltp):
        od = self.get_order_defaults(
            instrument=self.iput,
            quantity=self.put_quantity,
            expiry=self.expiry,
            option_type=OPTION_TYPE_PUT,
            strike=self.put_strike,
        )
        od["price"] = ltp
        res = dhan.place_order(**od)
        if res["status"] == "success":
            self.put_order = res["data"]
            print(self.put_order)
            self.put_od = od
            return True
        else:
            print(f"failed to place order: {res}, od: {od}")
            return False

    def wait_for_execution(self, order_id):
        start_time = dt.datetime.now()
        while (dt.datetime.now() - start_time) <= dt.timedelta(
            seconds=self.order_timeout
        ):
            order_details = dhan.get_order_by_id(order_id)
            if (
                order_details["status"] == "success"
                and order_details["data"]["orderStatus"] == "TRADED"
            ):
                return True
            time.sleep(0.1)
        return False

    def cancel_order(self, order_id):
        res = dhan.cancel_order(order_id)
        if res["status"] != "success":
            res = dhan.cancel_order(order_id)
        if res["status"] != "success":
            print(f"failed to cancel the order: {order_id}")
            # raise Exception(f"failed to cancel the order: {order_id}")

    def sell_order(self, buy_order_details):
        print(f"od: {buy_order_details}")
        od = buy_order_details.copy()
        od["transaction_type"] = dhan.SELL
        od["transaction_type"] = dhan.BUY
        if od["drv_options_type"] == "CALL":
            od["price"] = ut.convert_float(od["price"] * (1 - self.call_tp / 100))
            self.call_sell_order = dhan.place_order(**od)
            print(self.call_sell_order)
        elif od["drv_options_type"] == "PUT":
            od["price"] = ut.convert_float(od["price"] * (1 - self.put_tp / 100))
            self.put_sell_order = dhan.place_order(**od)
            print(self.put_sell_order)
        else:
            print("invalid order type option")

    def square_off_call(self, ltp):
        pending_order = self.get_pending_sell_order()
        if not pending_order:
            return
        print(f"squaring off call at: {ltp}")
        modify = dhan.modify_order(
            order_id=pending_order["orderId"],
            price=ltp,
            order_type=dhan.LIMIT,
            leg_name=None,
            quantity=pending_order["quantity"],
            disclosed_quantity=pending_order["disclosedQuantity"],
            trigger_price=None,
            validity=dhan.DAY,
        )
        print(modify)
        self.call_order = None
        self.failure += 1
        return modify

    def square_off_put(self, ltp):
        pending_order = self.get_pending_sell_order()
        if not pending_order:
            return
        print(f"squaring off call at: {ltp}")
        modify = dhan.modify_order(
            order_id=pending_order["orderId"],
            price=ltp,
            order_type=dhan.LIMIT,
            leg_name=None,
            quantity=pending_order["quantity"],
            disclosed_quantity=pending_order["disclosedQuantity"],
            trigger_price=None,
            validity=dhan.DAY,
        )
        print(modify)
        self.put_order = None
        self.failure += 1
        return modify

    def square_off_all(self, ltp):
        self.square_off_call(ltp)
        self.square_off_put(ltp)

    def get_order_defaults(self, instrument, quantity, expiry, option_type, strike):
        order_details = {
            "security_id": str(instrument["SEM_SMST_SECURITY_ID"]),
            "exchange_segment": dhan.NSE_FNO,
            # "transaction_type": dhan.BUY,
            "transaction_type": dhan.SELL,
            "quantity": quantity,
            "order_type": dhan.LIMIT,
            "product_type": dhan.MARGIN,
            "price": None,
            # "disclosed_quantity": max(50, math.ceil(quantity * 0.31)),
            "validity": dhan.DAY,
            "drv_expiry_date": expiry.strftime("%Y-%m-%d"),
            "drv_options_type": "CALL" if option_type == OPTION_TYPE_CALL else "PUT",
            "drv_strike_price": strike,
            "tag": "h4-vol",
        }
        return order_details

    def call_buy_wait_sell(self, ltp):
        placed = self.buy_call(ltp=ltp)
        if not placed:
            return False
        executed = self.wait_for_execution(self.call_order["orderId"])
        if executed:
            # Place sell order at tp
            self.sell_order(self.call_od)
            return True
        else:
            # Cancel the buy order
            self.cancel_order(self.call_order["orderId"])
            return False

    def put_buy_wait_sell(self, ltp):
        placed = self.buy_put(ltp=ltp)
        if not placed:
            return
        executed = self.wait_for_execution(self.put_order["orderId"])
        if executed:
            # Place sell order at tp
            self.sell_order(self.put_od)
            return True
        else:
            # Cancel the buy order
            self.cancel_order(self.put_order["orderId"])
            return False

    def get_open_positions(self, position_type="all"):
        print(
            f"success: {self.success}, failure: "
            f"{self.failure}, total: {self.success + self.failure}"
        )
        positions = dhan.get_positions()
        long = []
        short = []
        if len(positions["data"]) == 0:
            return []
        for position in positions["data"]:
            if position["positionType"] == "CLOSED":
                continue
            elif position["positionType"] == "LONG":
                long.append(position)
            elif position["positionType"] == "SHORT":
                short.append(position)
        if position_type == "all":
            return long + short
        elif position_type == "long":
            return long
        elif position_type == "short":
            return short
        else:
            raise NotImplementedError

    def get_pending_sell_order(self):
        orders = dhan.get_order_list()
        if orders is None or "data" not in orders or len(orders["data"]) == 0:
            return
        for order in orders["data"]:
            # if order["orderStatus"] == "PENDING" and order["transactionType"] == "SELL":
            if order["orderStatus"] == "PENDING" and order["transactionType"] == "BUY":
                return order


if __name__ == "__main__":
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
                config.call_buy_wait_sell()
            elif config.mode == MODE_PUT:
                config.put_buy_wait_sell()
            else:
                print("invalid mode set to make a buy")
        elif command == "fc":  # Buy call
            config.call_buy_wait_sell()
        elif command == "fp":  # Buy put
            config.put_buy_wait_sell()
        elif command == "fa":  # Buy both call and put
            config.call_buy_wait_sell()
            config.put_buy_wait_sell()
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
