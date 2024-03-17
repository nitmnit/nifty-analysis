from collections import deque
import pandas as pd
from typing import Dict
from logger_settings import logger


"""
class Tick:
    START_TIME = None

    def __init__(
        self,
        last_price,
        last_traded_quantity,
        total_buy_quantity,
        total_sell_quantity,
        last_trade_time,
        volume,
        oi=None,
    ):
        self.last_price = last_price
        self.last_traded_quantity = last_traded_quantity
        self.total_buy_quantity = total_buy_quantity
        self.total_sell_quantity = total_sell_quantity
        self.last_trade_time = last_trade_time
        self.volume = volume
        self.oi = oi
        if Tick.START_TIME is None:
            # This is with the assumption that the first tick will always be the farthest time
            Tick.START_TIME = last_trade_time.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        self.id = (last_trade_time - Tick.START_TIME).seconds

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"Tick: {self.id} {round(self.last_price, 2)}, v: {self.volume}"

"""


class Strategy:
    def __init__(self, instrument: "Instrument", settings: Dict):
        self.instrument = instrument
        self.ticks = None
        for key, val in settings.items():
            setattr(self, key, val)

    def next(self, tick):
        self._process_tick(tick)

    def _process_tick(self, tick: Dict) -> None:
        tick_df = pd.DataFrame(
            tick, index=[len(self.ticks) if self.ticks is not None else 0]
        )
        self.ticks = pd.concat([self.ticks, tick_df], ignore_index=True)


class Instrument:
    def __init__(self, name):
        self.name = name


class BasePhase:
    TYPE_CONT = "continuation"
    TYPE_STAG = "stagnation"
    STATUS_INITIATED = "INITIATED"
    STATUS_CONFIRMED = "CONFIRMED"
    STATUS_RETRACING = "RETRACING"
    STATUS_TERM = "TERMINATED"
    STATUS_REJECTED = "REJECTED"

    def __init__(self, initiated_at, strategy):
        self.status = self.STATUS_INITIATED
        self.direction = None
        self.type = self.TYPE_CONT
        self.initiated_at = initiated_at
        self.confirmed_at = None
        self.rejected_at = None
        self.term_at = None  # Terminated at
        self.retraced_at = None
        self.strategy = strategy

    def next(self, tick):
        raise NotImplementedError

    def __repr__(self):
        return f"Phase:{self.direction}, {self.status} I:{self.initiated_at}, C:{self.confirmed_at} R:{self.retraced_at}, T:{self.term_at}, RJ:{self.rejected_at}"

    def __str__(self):
        return self.__repr__()


class Order:
    ORDER_ID = 0
    TYPE_BUY = "buy"
    TYPE_SELL = "sell"
    STATUS_CREATED = "CREATED"
    STATUS_EXECUTED = "EXECUTED"
    STATUS_DECLINE = "DECLINED"
    STATUS_INTRADE = "INTRADE"
    STATUS_CLOSED = "CLOSED"

    def __init__(self, type, limit_price, quantity, created_at, exchange_order_id):
        self.type = type
        self.limit_price = limit_price
        self.quantity = quantity
        self.status = Order.STATUS_CREATED
        self.square_off_price = None
        self.id = Order.ORDER_ID
        self.created_at = created_at
        self.square_off_at = None
        self.exchange_order_id = exchange_order_id
        Order.ORDER_ID += 1

    def square_off(self, price, square_off_at):
        if self.status != Order.STATUS_INTRADE:
            raise Exception(f"order status not in trade: {self.status}")
        self.square_off_price = price
        self.square_off_at = square_off_at
        self.status = Order.STATUS_CLOSED

    @property
    def pnl(self):
        profit = self.quantity * \
            round(self.square_off_price - self.limit_price, 2)
        if self.type == Order.TYPE_BUY:
            return profit
        return -profit

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{self.type}, b:{self.limit_price}, s:{self.square_off_price}, pnl:{self.pnl}"


class PhaseOrder:
    def __init__(self, phase, order):
        self.phase = phase
        self.order = order

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{self.order}, Phase: {self.phase}"


class BasePhaseStrategy(Strategy):
    def __init__(self, instrument, settings):
        super().__init__(instrument=instrument, settings=settings)
        self.current_phase = None
        self.active_ph = []  # phase stack
        self.inactive_ph = []  # archived phase stack
        self.current_id = 0
        self.current_order = None
        self.closed_orders = []

    def next(self, tick):
        raise NotImplementedError

    def on_phase_confirmed(self):
        pass

    def on_phase_retracel(self):
        pass

    def on_termination(self):
        pass

    def on_initiation(self):
        pass

    def on_ongoing(self):
        pass
