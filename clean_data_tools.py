import raw_data_tools as rdt
import pandas as pd


def get_clean_data(item_name, dates, ids, *args):
    # Try to find if a corresponding clean data function is available.
    # If not, call the raw data function
    print('\nRetrieving Data Item %s from the Database...' % item_name)
    try:
        func_name = eval('get_' + item_name.lower())
        data = func_name(dates, ids, *args)
    except NameError:
        data = rdt.get_raw_data(item_name, dates, ids)
    data = data.round(2)
    return data


def get_returnusd(dates, ids):
    new_dates = dates.union([dates[0]-1])   # Go back one period
    price = get_clean_data('PriceUsdAdj', new_dates, ids)
    data = 1 + price.pct_change()
    data = data.fillna(1)
    data = data.reindex(index=dates)
    return data


def get_returnlocal(dates, ids):
    new_dates = dates.union([dates[0]-1])    # Go back one period
    price = get_clean_data('PriceLocalAdj', new_dates, ids)
    data = 1 + price.pct_change()
    data = data.fillna(1)
    data = data.reindex(index=dates)
    return data


def get_volumeusd(dates, ids):
    volume_shares = get_clean_data('VolumeSharesAdj', dates, ids)
    price_usd = get_clean_data('PriceUsdAdj', dates, ids)
    volume_usd = volume_shares * price_usd
    return volume_usd


def get_volumelocal(dates, ids):
    volume_shares = get_clean_data('VolumeSharesAdj', dates, ids)
    price_local = get_clean_data('PriceLocalAdj', dates, ids)
    volume_local = volume_shares * price_local
    return volume_local


def get_securitymcapusd(dates, ids):
    mcap = rdt.get_raw_data('SecurityMcapUsd', dates, ids)
    mcap = mcap * 1e6       # Mcap is stored in millions in the database
    return mcap


def get_adv(dates, ids, ndays):
    # convert dates into daily frequency
    new_dates = pd.date_range(dates.min() - pd.DateOffset(ndays), dates.max(), freq='D')
    volume_usd = get_clean_data('VolumeUsd', new_dates, ids)
    adv = volume_usd.rolling(axis=0, window=ndays, min_periods=ndays//2).mean()
    adv = adv.reindex(index=dates, columns=ids)
    return adv


