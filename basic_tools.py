
import pandas as pd
import datetime
from pandas.tseries.offsets import *


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

