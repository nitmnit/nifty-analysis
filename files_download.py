import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

def fetch_nifty_futures_data(start_date, end_date, folder_path):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'DNT': '1', 
        'Upgrade-Insecure-Requests': '1'
    }
    
    def generate_dates(start_date, end_date):
        current_date = start_date
        dates = []
        while current_date <= end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)
        return dates

    def download_file(date, folder_path, file_type):
        if file_type == "fii_stats":
            file_name = f"fii_stats_{date.strftime('%d-%b-%Y')}.xls"
            url = f"https://nsearchives.nseindia.com/content/fo/fii_stats_{date.strftime('%d-%b-%Y')}.xls"
        elif file_type == "oi_data":
            file_name = f"fao_participant_oi_{date.strftime('%d%m%Y')}.csv"
            url = f"https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_{date.strftime('%d%m%Y')}.csv"
        
        file_path = os.path.join(folder_path, file_name)

        if os.path.exists(file_path):
            if os.path.getsize(file_path) == 0:
                return None
            else:
                return file_path
        else:
            for attempt in range(5):
                try:
                    response = requests.get(url, headers=headers, timeout=10)
                    if response.status_code == 200:
                        with open(file_path, 'wb') as file:
                            file.write(response.content)
                        return file_path
                    else:
                        with open(file_path, 'wb') as file:
                            pass
                        return None
                except (requests.ConnectionError, requests.Timeout):
                    time.sleep(2)
            return None

    def collect_data(start_date, end_date, folder_path, file_type):
        all_data = []
        dates = generate_dates(start_date, end_date)
        for date in dates:
            file_path = download_file(date, folder_path, file_type)
            if file_path:
                if file_type == "fii_stats":
                    df = pd.read_excel(file_path)
                    nifty_futures_row = df[df.iloc[:, 0] == "NIFTY FUTURES"]
                    if not nifty_futures_row.empty:
                        row_values = nifty_futures_row.values.flatten().tolist()[1:]
                        row_values.append(date.strftime('%Y-%m-%d'))
                        all_data.append(row_values)
                elif file_type == "oi_data":
                    df = pd.read_csv(file_path, skiprows=1)
                    df.columns = df.columns.str.replace(' ', '_').str.lower()
                    # df = df.reset_index(drop=True)
                    df = df[:-1]
                    df['date'] = date.strftime('%Y-%m-%d')
                    all_data.append(df)
        if all_data:
            if file_type == "fii_stats":
                columns = ["buy_contracts", "buy_amount", "sell_contracts", "sell_amount", "eod_oi_contracts", "eod_oi_amount", "date"]
                combined_data = pd.DataFrame(all_data, columns=columns)
                combined_data.set_index("date", inplace=True)
                return combined_data
            elif file_type == "oi_data":
                combined_data = pd.concat(all_data, ignore_index=True)
                combined_data.set_index("date", inplace=True)
                return combined_data
        else:
            return pd.DataFrame()

    os.makedirs(folder_path, exist_ok=True)
    
    futures_data = collect_data(start_date, end_date, folder_path, "fii_stats")
    oi_data = collect_data(start_date, end_date, folder_path, "oi_data")
    
    return futures_data, oi_data

