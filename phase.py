import datetime as dt
from logger_settings import logger
from collections import deque


START_TIME = dt.datetime.now().replace(year=2024, month=3, day=1, hour=0, minute=0, second=0, microsecond=0)

class Tick:
    def __init__(self, last_price, last_traded_quantity, total_buy_quantity, total_sell_quantity, last_trade_time, volume, oi):
        self.last_price = last_price
        self.last_traded_quantity = last_traded_quantity
        self.total_buy_quantity = total_buy_quantity
        self.total_sell_quantity = total_sell_quantity
        self.last_trade_time = last_trade_time
        self.volume = volume
        self.oi = oi
        self.id = (last_trade_time - START_TIME).seconds

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"Tick: {self.id} {round(self.last_price, 2)}, v: {self.volume}"


class Direction:
    UP = 'up'
    DOWN = 'down'

class Instrument:
    def __init__(self, name):
        self.name = name

class Phase:
    TYPE_CONT = "continuation"
    TYPE_STAG = "stagnation"
    STATUS_INITIATED = 'INITIATED'
    STATUS_STARTED = 'STARTED'
    STATUS_TERM = 'TERMINATED'
    STATUS_ONG = 'ONGOING'
    STATUS_SOFT_RETR = 'SOFT RETRACING'
    STATUS_HARD_RETR = 'HARD RETRACING'

    def __repr__(self):
        #return f"Continuation: {self.direction}, {self.status} from {self.t_start} to {self.t_end}, confirmation:{self.started_at} terminated at: {self.terminated_at}, hard retracel: {self.hard_retraced_at}, soft: {self.soft_retraced_at}"
        return f"Continuation: {self.direction}, {self.status} from {self.t_start} to {self.t_end}, confirmation:{self.started_at} terminated at: {self.terminated_at}, hard retracel: {self.hard_retraced_at}, soft: {self.soft_retraced_at}, conf: {self.confidence_at_confirmation}"

    def __str__(self):
        return self.__repr__()

    def get_last_n_high(self):
        """
        """
        if len(self.pm.ticks) < 2:
            raise Exception(f"insufficient length to calculate last n high len: {len(self.pm.ticks)}")
        if self.direction == Direction.UP:
            return min(self.pm.ticks[-2-self.pm.settings.LAST_TICKS_BREACH[self.direction]: -2], key=lambda x: x.last_price).last_price
        elif self.direction == Direction.DOWN:
            return max(self.pm.ticks[-2-self.pm.settings.LAST_TICKS_BREACH[self.direction]: -2], key=lambda x: x.last_price).last_price
        else:
            raise NotImplementedError

    def get_last_nsec_ticks(self, n):
        ticks = []
        close_id = self.last_5sec[-1].id
        idx_sl = 0 # index in second last
        start_id = self.second_last_5sec[idx_sl].id
        for idx_sl in range(len(self.second_last_5sec)):
            if close_id - self.second_last_5sec[idx_sl].id <= n:
                ticks.append(self.second_last_5sec[idx_sl])
        for tick in self.last_5sec:
            ticks.append(tick)
        return ticks

    def update_last_nsec(self, tick):
        """
        Updates the state for last_5sec and second_last_5sec when new ticks arrive
        """
        self.last_5sec.append(tick)
        if self.second_last_5sec.period < self.pm.settings.CONFIRM_TICKS:
            x = self.last_5sec.popleft()
            self.second_last_5sec.append(x)
            return
        while self.last_5sec.period > self.pm.settings.CONFIRM_TICKS:
            x = self.last_5sec.popleft()
            self.second_last_5sec.append(x)
        while self.second_last_5sec.period > self.pm.settings.CONFIRM_TICKS:
            self.second_last_5sec.popleft()


class PhaseStartedException(Exception):
    pass

class PhaseSoftRetraceException(Exception):
    pass

class PhaseHardRetraceException(Exception):
    pass

class PhaseTerminatedException(Exception):
    pass


class Order:
    ORDER_ID = 0

    TYPE_BUY = 'buy'
    TYPE_SELL = 'sell'
    STATUS_CREATED = 'CREATED'
    STATUS_EXECUTED = 'EXECUTED'
    STATUS_DECLINE = 'DECLINED'
    STATUS_INTRADE = 'INTRADE'
    STATUS_CLOSED = 'CLOSED'

    def __init__(self, type, limit_price, quantity):
        self.type = type
        self.limit_price = limit_price
        self.quantity = quantity
        self.STATUS = 'CREATED'
        self.square_off_price = None
        self.id = Order.ORDER_ID
        Order.ORDER_ID += 1

    def square_off(self, price):
        self.square_off_price = price
        self.STATUS = Order.STATUS_CLOSED

    @property
    def pnl(self):
        profit = round(self.square_off_price - self.limit_price,  2)
        if self.type == Order.TYPE_BUY:
            return profit
        return - profit

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


class Candle(deque):
    def __init__(self, *args, **kwargs):
        self.confirm_ticks = kwargs.pop('confirm_ticks')
        super().__init__(*args, **kwargs)
        self.HIGH = self.OPEN
        self.LOW = self.OPEN

    def append(self, x):
        super().append(x)
        if len(self) == 1:
            self.HIGH = x.last_price
            self.LOW = x.last_price
        else:
            self.HIGH = max(x.last_price, self.HIGH)
            self.LOW = min(x.last_price, self.LOW)
    
    @property
    def volume(self):
        if len(self) == 0:
            return 0
        return sum([t.volume for t in self]) / len(self)

    def popleft(self):
        x = super().popleft()
        if len(self) > 0 and self.HIGH == x.last_price:
            self.HIGH = max(self, key=lambda k: k.last_price).last_price
        if len(self) > 0 and self.LOW == x.last_price:
            self.LOW = min(self, key=lambda k: k.last_price).last_price
        return x

    @property
    def OPEN(self):
        if len(self) == 0:
            return
        return self[0].last_price

    @property
    def CLOSE(self):
        if len(self) == 0:
            return
        return self[-1].last_price

    @property
    def IS_RED(self):
        return self.OPEN > self.CLOSE

    @property
    def period(self):
        if len(self) == 0:
            return 0
        return self[-1].id - self[0].id

    @property
    def start_id(self):
        if len(self) == 0:
            return
        return self[0].id

    @property
    def end_id(self):
        if len(self) == 0:
            return
        return self[-1].id

    @property
    def confidence(self):
        if len(self) <= 1:
            return 0
        up_sum = 0
        down_sum = 0

        total = (up_sum + abs(down_sum))
        if total == 0:
            return 0
        for i in range(1, len(self)):
            change = self[i].last_price - self[i-1].last_price
            if change >= 0:
                up_sum += change
            else:
                down_sum += change
        return up_sum * 100 / (up_sum + abs(down_sum))


class PhaseStartFailed(Exception):
    pass


class PhaseManager:
    def __init__(self, instrument, settings):
        self.instrument = instrument
        self.current_phase = None
        self.ps = [] # phase stack
        self.aps = [] # archived phase stack
        self.ticks = []
        self.first_pc = None
        self.current_id = 0
        self.current_order = None
        self.closed_orders = []
        self.settings = settings

    def get_pc_tick(self, tick):
        if len(self.ticks) == 0:
            self.first_pc = tick
#        pc_tick = {
#            'last_price': (tick['last_price'] - self.first_pc['last_price']) * 100 / self.first_pc['last_price'],
#            'last_traded_quantity': (tick['last_traded_quantity'] - self.first_pc['last_traded_quantity']) * 100 / self.first_pc['last_traded_quantity'],
#            'total_buy_quantity': (tick['total_buy_quantity'] - self.first_pc['total_buy_quantity']) * 100 / self.first_pc['total_buy_quantity'],
#            'total_sell_quantity': (tick['total_sell_quantity'] - self.first_pc['total_sell_quantity']) * 100 / self.first_pc['total_sell_quantity'],
#            'last_trade_time': tick['last_trade_time'],
#            'volume': tick['volume'],
#            'oi': (tick['oi'] - self.first_pc['oi']) * 100 / self.first_pc['oi'],
#        }
        pc_tick = {
            'last_price': tick['last_price'],
            'last_traded_quantity': tick['last_traded_quantity'],
            'total_buy_quantity': tick['total_buy_quantity'],
            'total_sell_quantity': tick['total_sell_quantity'],
            'last_trade_time': tick['last_trade_time'],
            'volume': tick['volume'],
            'oi': tick['oi'],
        }
        tick_obj = Tick(**pc_tick)
        self.current_id += 1
        return tick_obj

    def next(self, tick):
        pc_tick = self.get_pc_tick(tick)
        self.ticks.append(pc_tick)
        return self.process(pc_tick)

    def process_ps(self, tick):
        # Process for process stack
        mark_for_removal = []
        for i in range(len(self.ps)):
            phase = self.ps[i]
            try:
                phase.process(tick)
            except PhaseSoftRetraceException:
                # logger.info(f"soft retracel of {phase}")
                pass
            except PhaseHardRetraceException:
                pass
            except PhaseTerminatedException:
                mark_for_removal.append(i)
                self.aps.append(phase)
        mark_for_removal.reverse()
        for mark in mark_for_removal:
            del self.ps[mark]

    def on_soft_retracel(self, phase, tick):
        # logger.info(f"soft retracel {phase}")
        pass

    def on_phase_start(self, phase, tick):
        self.current_order = Order(type=Order.TYPE_BUY, limit_price=tick.last_price, quantity=self.settings.QUANTITY)

    def on_hard_retracel(self, phase, tick):
        if phase.direction == Direction.UP:
            if self.current_order is not None:
                self.current_order.square_off(tick.last_price)
                self.closed_orders.append(PhaseOrder(phase, self.current_order))
                self.current_order = None
            logger.info(f"hard retracel {phase}, Up change %: {phase.hard_retraced_at.last_price - phase.started_at.last_price}")
        else:
            logger.info(f"hard retracel {phase}, Down change %: {phase.started_at.last_price - phase.hard_retraced_at.last_price}")

    def on_termination(self, phase, tick):
        # logger.info(f"termination {phase}")
        pass

    def on_initiation(self, phase, tick):
        # logger.info(f"initiation {phase}")
        pass

    def on_ongoing(self, phase, tick):
        pass
        # logger.info(f"ongoing {phase}")

