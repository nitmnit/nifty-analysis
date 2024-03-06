from unittest import TestCase
import utils as ut
import datetime as dt
from constants import *
import kite_gapup as kg


class UtilTest(TestCase):
    def test_get_data_interval(self):
        from_dt = dt.datetime(year=2024, month=1, day=3).date()
        to = dt.datetime(year=2024, month=1, day=4).date()
        data = ut.get_data_interval(symbol='NIFTY 50', from_date=from_dt, to_date=to, interval=INTERVAL_MIN1, exchange=EXCHANGE_NSE)
        self.assertIsNotNone(data)

    def test_get_instrument_details(self):
        expiry = dt.datetime(year=2024, month=3, day=28).date()
        strike = 1450
        symbol = 'HDFCBANK'
        otype = 'CE'
        instrument = ut.get_fo_instrument_details(symbol=symbol, expiry=expiry, strike=strike, option_type=otype, exchange=EXCHANGE_NSE)
        self.assertEqual(instrument["SEM_SMST_SECURITY_ID"], 122967)

    def test_get_nifty_exchange_token(self):
        expiry = dt.datetime(year=2024, month=3, day=7).date()
        strike = 23000
        symbol = 'NIFTY'
        otype = 'CE'
        instrument = ut.get_fo_instrument_details(symbol=symbol, expiry=expiry, strike=strike, option_type=otype, exchange=EXCHANGE_NSE)
        self.assertEqual(instrument["SEM_SMST_SECURITY_ID"], 41717)


class DhanGapupTest(TestCase):
    def test_gapup_trade(self):
        is_live = "test" == "live"
        buy_date = dt.datetime.strptime("2024-01-04", "%Y-%m-%d").date()
        sell_date = dt.datetime.strptime("2024-01-05", "%Y-%m-%d").date()
        expiry = dt.datetime.strptime("2024-03-07", "%Y-%m-%d").date()

        prs = {
                "nifty_price_time": dt.time(hour=15, minute=20),
                "buy_at": dt.time(hour=15, minute=26),
                "kite_symbol": "NIFTY 50",
                "symbol": "NIFTY",
                "interval": INTERVAL_MIN1,
                "exchange": EXCHANGE_NSE,
                "strike_i": -5,
                "is_live": is_live,
                "buy_date": buy_date,
                "sell_date": sell_date,
                "expiry": expiry,
                "quantity": 10,
                "option_type": OPTION_TYPE_CALL,
                "exchange_segment_enum": DHAN_EXCHANGE_SEGMENTS_ENUM["NSE_FNO"],
                "is_entry": True, 
        }

        od = kg.get_order_details(prs)
        kg.subscribe(prs, od)

