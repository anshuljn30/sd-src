
import pandas as pd
import datetime
from pandas.tseries.offsets import *
import xlwings as xw

def fill_forward(df,column_from):
    for i in range (0,len(df.index)):
        for j in range(column_from+1,len(df.columns)):
            if(df.ix[i,j] == None): df.ix[i,j] = df.ix[i,j-1]
    return df

def write_to_sheet(df, file, sheet,open_excel):
    if (open_excel==False):
        app = xw.App(visible=False)
    wb = xw.Book(file)
    sht = wb.sheets[sheet]
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

