import raw_data_tools as rdt
import pandas as pd


def get_clean_data(item_name, dates, ids):
    # Try to find if a corresponding clean data function is available.
    # If not, call the raw data function
    print('\nRetrieving Data Item %s from the Database...' % item_name)
    try:
        func_name = eval('get_' + item_name.lower())
        data = func_name(dates, ids)
    except NameError:
        data = rdt.get_raw_data(item_name, dates, ids)
    data = data.round(2)
    return data


def get_ebit(dates, ids):
    data = rdt.get_raw_data('EBIT', dates, ids)
    return data


def get_returnusd(dates, ids):
    new_dates = dates.union([dates[0]-1])   # Go back one period
    price = get_clean_data('PriceUsdAdj', new_dates, ids)
    data = 1 + price.pct_change()
    data = data.reindex(index=dates)
    return data


def get_returnlocal(dates, ids):
    new_dates = dates.union([dates[0]-1])    # Go back one period
    price = get_clean_data('PriceLocalAdj', new_dates, ids)
    data = 1 + price.pct_change()
    data = data.reindex(index=dates)
    return data

