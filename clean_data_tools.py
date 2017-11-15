import raw_data_tools as rdt
import pandas as pd


def get_clean_data(item_name, **kwargs):
    # Two ways to call this function
    # eps = cdt.get_clean_data('EPS', dates=dates, ids=ids)
    # eps = cdt.get_clean_data('EPS', universe=univ)
    # where dates is a pd.DataTimeObject representing a list of dates,
    # ids is a list of numeric ids
    # universe is a logical data frame of size dates x ids

    if 'dates' in kwargs and 'ids' in kwargs:
        dates = kwargs['dates']
        ids = kwargs['ids']
        del kwargs['dates']
        del kwargs['ids']
        universe = pd.DataFrame(True, index=dates, columns=ids)

    elif 'universe' in kwargs:
        universe = kwargs['universe']
        del kwargs['universe']
        dates = universe.index
        ids = list(universe)

    else:
        raise ValueError('Specify dates & ids or a universe data frame')

    # Try to find if a corresponding clean data function is available.
    # If not, call the raw data function
    print('\nRetrieving Data Item %s from the Database...' % item_name)
    try:
        func_name = eval('get_' + item_name.lower())
        data = func_name(dates, ids, **kwargs)
    except NameError:
        data = rdt.get_raw_data(item_name, dates, ids, **kwargs)

    # Return only those dates and ids which are in universe
    data = data[universe]
    data = data.round(4)
    return data


def get_return_usd(dates, ids):
    new_dates = dates.union([dates[0]-1])   # Go back one period
    price = get_clean_data('price_usd_adj', dates=new_dates, ids=ids)
    data = 1 + price.pct_change()
    data = data.fillna(1)
    data = data.reindex(index=dates)
    return data


def get_return_local(dates, ids):
    new_dates = dates.union([dates[0]-1])    # Go back one period
    price = get_clean_data('price_local_adj', dates=new_dates, ids=ids)
    data = 1 + price.pct_change()
    data = data.fillna(1)
    data = data.reindex(index=dates)
    return data


def get_volume_usd(dates, ids):
    volume_shares = get_clean_data('volume_shares_adj', dates=dates, ids=ids)
    price_usd = get_clean_data('price_usd_adj', dates=dates, ids=ids)
    volume_usd = volume_shares * price_usd
    return volume_usd


def get_volume_local(dates, ids):
    volume_shares = get_clean_data('volume_shares_adj', dates=dates, ids=ids)
    price_local = get_clean_data('price_local_adj', dates=dates, ids=ids)
    volume_local = volume_shares * price_local
    return volume_local


def get_security_mcap_usd(dates, ids):
    mcap = rdt.get_raw_data('security_mcap_usd', dates, ids)
    mcap = mcap * 1e6       # Mcap is stored in millions in the database
    return mcap


def get_adv(dates, ids, window=250):
    # convert dates into daily frequency
    new_dates = pd.date_range(dates.min() - pd.DateOffset(window), dates.max(), freq='D')
    volume_usd = get_clean_data('volume_usd', dates=new_dates, ids=ids)
    adv = volume_usd.rolling(axis=0, window=window, min_periods=window//2).mean()
    adv = adv.reindex(index=dates, columns=ids)
    return adv


