from collections import deque
import pandas as pd
from typing import Dict, List
from logger_settings import logger


class Strategy:
    def __init__(self, instrument: "Instrument", settings: Dict):
        self.instrument = instrument
        self.ticks: pd.DataFrame | None = None
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
    STATUS_INITIATED = "INITIATED"
    STATUS_STAGNATING = "STAGNATING"
    STATUS_CONFIRMED = "CONFIRMED"
    STATUS_RETRACING = "RETRACING"
    STATUS_TERM = "TERMINATED"
    STATUS_REJECTED = "REJECTED"
    ID = 0
    ONGOING_STATUS = [STATUS_INITIATED, STATUS_CONFIRMED]

    def __init__(self, initiated_at, strategy, previous_phase):
        self.status = self.STATUS_INITIATED
        self.direction = None
        self.type = self.TYPE_CONT
        self.initiated_at = initiated_at
        self.resumed_at = initiated_at
        self.ended_at = strategy.ticks.iloc[-1].name
        self.confirmed_at = None
        self.rejected_at = None
        self.term_at = None  # Terminated at
        self.retraced_at = None
        self.strategy = strategy
        self.previous_phase = previous_phase
        self.created_at = strategy.ticks.iloc[-1].name
        self.id = BasePhase.ID
        self.term_reason = None
        self.retrace_reason = None
        BasePhase.ID += 1
        self.pticks: pd.DataFrame | None = None  # Phase ticks since last resumed_at
        self.aticks: pd.DataFrame | None = None  # All phase ticks

    def next(self):
        self.ended_at = self.strategy.ticks.iloc[-1].name
        self.pticks = self.strategy.ticks.loc[self.resumed_at:].copy()
        self.aticks = self.strategy.ticks.loc[self.initiated_at:].copy()

    @property
    def length(self):
        return self.ended_at - self.resumed_at

    def __repr__(self):
        return f"Phase {self.id}:{self.direction}, {self.status} I:{self.initiated_at}, C:{self.confirmed_at} R:{self.retraced_at}, T:{self.term_at}, RJ:{self.rejected_at}, cr:{self.created_at} tr: {self.term_reason}, rr:{self.retrace_reason}, end:{self.ended_at}"

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
        if self.square_off_price is None:
            return
        profit = self.quantity * \
            round(self.square_off_price - self.limit_price, 2)
        if self.type == Order.TYPE_BUY:
            return profit
        return -profit

    @property
    def pnl_pc(self):
        if self.square_off_price is None:
            return
        pc_profit = (self.square_off_price - self.limit_price) * \
            100 / self.limit_price
        if self.type == Order.TYPE_BUY:
            return pc_profit
        return -pc_profit

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{self.type}, at:{self.created_at}, b:{self.limit_price}, sqat:{self.square_off_at}, s:{self.square_off_price}, pnl:{self.pnl_pc}"


class PhaseOrder:
    def __init__(self, phase, order):
        self.phase = phase
        self.order: Order = order

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{self.order}, Phase: {self.phase}"


class OrderManager:
    """
    High level utility to manager orders to keep it independent of underlying APIs
    """

    def __init__(self):
        self.orders: List[PhaseOrder] = []
        self.closed_orders = []

    def place_order(self, order):
        logger.info("place_order called")
        order.order.status = Order.STATUS_INTRADE
        self.orders.append(order)

    def has_intrade_orders(self, phase=None) -> bool:
        if phase is None:
            return len(self.orders) > 0
        for po in self.orders:
            if po.phase.id == phase.id:
                return True
        return False

    def square_off_all_orders(self, index, last_price, phase):
        for i in range(len(self.orders) - 1, -1, -1):
            order = self.orders[i]
            if order.phase.id == phase.id:
                order.order.square_off(last_price, index)
                logger.info(f"squared off {order.order}")
                self.closed_orders.append(order)
                del self.orders[i]


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
        super().next(tick)

    def on_confirmed(self, phase):
        pass

    def on_retracel(self, phase, reason):
        pass

    def on_termination(self, phase, reason):
        pass

    def on_initiation(self, phase):
        pass

    def on_ongoing(self, phase):
        pass
