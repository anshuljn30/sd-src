
import pandas as pd
import datetime
from pandas.tseries.offsets import *
import xlwings as xw
import numpy as np
import db_tools as dt
import clean_data_tools as cdt
import company as c


def random_frame(ndates, nids):
    dates = pd.date_range('20140131', periods=ndates, freq='m')
    ids = list(range(1,nids+1))
    data_matrix = np.random.randint(0, 100, size=(ndates, nids))/100
    data = pd.DataFrame(data_matrix, index=dates, columns=ids)
    return data


def convert_to(data, frequency):
    data = data.asfreq(frequency.lower(), method='ffill')
    return data


def get_universe(dates):
    ids = dt.get_all_security_ids()
    mcap = cdt.get_clean_data('security_mcap_usd', dates=dates, ids=ids)
    volume = cdt.get_clean_data('ADV', dates=dates, ids=ids, window=90)
    universe = (mcap > 1e5) & (volume > 1e4)
    universe = universe.loc[:, universe.any(axis=0)]   # trim ids which were never in the universe
    return universe


def aggregate(data, property, func, **kwargs):
    ids = list(data)
    company = c.Company(ids)
    company_property = getattr(company, property).set_index('issuer_id').T.to_dict(orient='records')[0]
    split_data = data.groupby(by=company_property, axis=1)

    if func == 'weighted_mean' and 'weights' in kwargs:
        w = kwargs['weights']
        w = w.div(w.sum(axis=1, skipna=True), axis=0)
        func = lambda x: (x*w).sum(axis=1, skipna=True)
    else:
        func = lambda x: x.func(axis=1, skipna=True)

    agg_data = split_data.apply(func)
    return agg_data


def fill_forward(df,column_from):
    for i in range (0,len(df.index)):
        for j in range(column_from+1,len(df.columns)):
            if(df.ix[i,j] == None): df.ix[i,j] = df.ix[i,j-1]
    return df


def write_to_sheet(df, file, sheet,open_excel):
    if (open_excel==False):
        app = xw.App(visible=False)
    wb = xw.Book(file)
    wb.app.display_alerts = False
    sht = wb.sheets[sheet]
    sht.clear()
    sht.range('A1').value = df
    sht.range('A1').options(pd.DataFrame, expand='table').value
    wb.save()
    if (open_excel==False):
        wb.close()


def eom(df,format):
    df = pd.to_datetime(df, format=format)
    df = df.apply(lambda x: x + pd.offsets.MonthEnd(0))
    df = df.apply(lambda x: x.strftime(format))
    return df


def eomb(df,format):
    df = pd.to_datetime(df, format=format)
    df = df.apply(lambda x: x + pd.offsets.MonthBegin(-1))
    df = df.apply(lambda x: x + pd.offsets.BMonthEnd(0))
    df = df.apply(lambda x: x.strftime(format))
    return df


def eoq(df,format):
    df = pd.to_datetime(df, format=format)
    df = df.apply(lambda x: x + pd.offsets.QuarterEnd(0))
    df = df.apply(lambda x: x.strftime(format))
    return df


def eoqb(df,format):
    df = pd.to_datetime(df, format=format)
    df = df.apply(lambda x: x + pd.offsets.MonthBegin(-1))
    df = df.apply(lambda x: x + pd.offsets.BQuarterEnd(0))
    df = df.apply(lambda x: x.strftime(format))
    return df


def eoy(df,format):
    df = pd.to_datetime(df, format=format)
    df = df.apply(lambda x: x + pd.offsets.YearEnd(0))
    df = df.apply(lambda x: x.strftime(format))
    return df


def eoyb(df,format):
    df = pd.to_datetime(df, format=format)
    df = df.apply(lambda x: x + pd.offsets.MonthBegin(-1))
    df = df.apply(lambda x: x + pd.offsets.BYearEnd(0))
    df = df.apply(lambda x: x.strftime(format))
    return df

