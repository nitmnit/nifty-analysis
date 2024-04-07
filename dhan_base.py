def get_instrument_file_name(instrument):
    return f"{instrument["SEM_TRADING_SYMBOL"]}.ltp"


def get_instrument_ltp(instrument):
    with open(get_instrument_file_name(instrument=instrument), "r") as f:
        price = f.read()
    return float(price)


def set_instrument_ltp(instrument, ltp):
    with open(get_instrument_file_name(instrument=instrument), "w+") as f:
        f.write(ltp)
