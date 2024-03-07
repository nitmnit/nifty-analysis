import pandas as pd


file_name = "22400-call.json"
df = pd.read_json(file_name)
ddf = df.data.copy()

data = {
        "open": [],
        "high": [],
        "low": [],
        "close": [],
        "volume": [],
        "time": [],
        "oi": [],
        "Time": []
    }

first = True
def get_data_df(row):
    global data, first
    if first or '09:15:00' not in row['Time'][0]:
        data['open'] += row['o']
        data['high'] += row['h']
        data['low'] += row['l']
        data['close'] += row['c']
        data['volume'] += row['v']
        data['time'] += row['t']
        data['Time'] += row['Time']
        data['oi'] += row['oi']
        if '2024-03-05T09:15:00+05:30' in row['Time'][0]:
            first = False


ddf.apply(get_data_df)
rdf = pd.DataFrame(data)
rdf.to_json('tv-22400-call.json')
