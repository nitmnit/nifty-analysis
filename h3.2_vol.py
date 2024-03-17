import base as b
import pandas as pd
import datetime as dt
from typing import Dict


class MyPhaseManager(b.PhaseManager):
    def __init__(self, instrument: "Instrument", settings: Dict):
        super().__init__(instrument, settings)

    def process_tick(self, tick: Dict) -> None:
        tick_df = pd.DataFrame(
            tick, index=[len(self.ticks) if self.ticks is not None else 0]
        )
        self.ticks = pd.concat([self.ticks, tick_df], ignore_index=True)

    def next(self, tick: Dict):
        self.process_tick(tick)


settings = {
    "test": "val",
}

instrument = b.Instrument(name="NIFTY 22650 CALL 7 Mar 2024")
pm = MyPhaseManager(instrument=instrument, settings=settings)


ticks_columns = {
    "last_price": float,
    "last_traded_quantity": int,
    "total_buy_quantity": int,
    "total_sell_quantity": int,
    "last_trade_time": "datetime64[ns]",
    "volume_traded": "int",
    "oi": int,
}
tdf = pd.read_json(
    "ticks-formatted.json",
    dtype=ticks_columns,
    convert_dates={"last_trade_time": "%Y-%m-%d %H:%M:%S"},
)
columns_to_drop = set(tdf.columns).difference(ticks_columns.keys())
tdf.drop(columns=columns_to_drop, inplace=True)
tdf.rename(columns={"volume_traded": "day_volume_traded", "oi": "day_oi"}, inplace=True)
tdf.set_index("last_trade_time", inplace=True, drop=False)
tdf["volume"] = tdf.day_volume_traded - tdf.day_volume_traded.shift(1)
tdf.fillna({"volume": 0}, inplace=True)
tdf["volume"] = tdf.volume.astype("int")
tdf.drop("day_volume_traded", inplace=True, axis=1)
tdf.drop("day_oi", inplace=True, axis=1)


# for i in range(tdf.shape[0]):
for i in range(10):
    pm.next(tdf.iloc[i].to_dict())


print(pm.ticks)
print(pm.ticks.dtypes)
