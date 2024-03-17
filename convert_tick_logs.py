import json
import pandas as pd
import os
import historical_data as hd
from constants import *
import utils as ut


def build_data():
    directory = "."
    data_dir = "data/ticks/"
    ku = hd.KiteUtil(exchange=EXCHANGE_NFO)
    for filename in os.listdir(directory):
        if not (filename.startswith("tick_") and filename.endswith(".log")):
            continue
        if filename != "tick_2024-03-15.log":
            continue
        file_path = os.path.join(directory, filename)
        print(file_path)
        results = []
        with open(file_path, "r") as file:
            for line in file:
                if not line.startswith('[{"tradable"'):
                    continue
                results += json.loads(line)
        df = pd.DataFrame(results)
        df["last_trade_time"] = pd.to_datetime(df["last_trade_time"])
        gdf = df.groupby("instrument_token")
        for it, idf in gdf:
            if it == NIFTY_ITOKEN:
                continue
            instrument = ku.get_ft_instrument_from_it(it)
            if instrument is None:
                print(f"instrument token not found: {it}")
                out_file_path = (
                    f"data/ticks/{it}-{idf.iloc[0].last_trade_time.date()}.csv"
                )
            else:
                out_file_path = ut.get_tick_file_path(
                    symbol=instrument["name"],
                    expiry=instrument["expiry"],
                    strike=instrument["strike"],
                    type=instrument["instrument_type"],
                    date=idf.iloc[0].last_trade_time.date(),
                )
            print(out_file_path)
            idf.to_csv(out_file_path)


if __name__ == "__main__":
    build_data()
