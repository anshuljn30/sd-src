import pandas as pd
import numpy as np
import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta
import signal_test_tools
import basic_tools

def jitter(series, noise_reduction=100000000):
    return (np.random.random(len(series))*series.std()/noise_reduction)-(series.std()/(2*noise_reduction))

def fractile_returns(scores, returns, fractile):
    date_scores = scores.columns[1]
    date_returns = returns.columns[1]
    scores['fractiles'] = pd.DataFrame(list(pd.qcut(scores[date_scores] + jitter(scores[date_scores]), fractile, labels=False, retbins=True)[0:1])).T
    df_scores = scores.loc[:, ['id', 'fractiles']]
    df = pd.merge(df_scores ,returns ,on='id' ,how='left')
    fractile_returns = pd.DataFrame(list(df.groupby(by='fractiles')[date_returns].mean()))
    fractile_returns.columns = [date_scores]
    return fractile_returns.T

def fractile_correlation(scores, returns, fractile,by_sector):
    date_scores = scores.columns[1]
    date_returns = returns.columns[1]
    scores['fractiles'] = pd.DataFrame(list(pd.qcut(scores[date_scores] + jitter(scores[date_scores]), fractile, labels=False, retbins=True)[0:1])).T
    df_scores = scores.loc[:, ['id', 'fractiles',date_scores,'gics_sector']]
    df_scores = df_scores.rename(columns={date_scores: 's' + date_scores})
    df_returns = returns.loc[:,['id', date_returns]]
    df_returns = df_returns.rename(columns={date_returns: 'r' + date_returns})
    df = pd.merge(df_scores, df_returns, on='id', how='left')
    if(by_sector==True):
        fractile_correlation = df.groupby(by='gics_sector')[['s' + date_scores, 'r' + date_returns]].corr().ix[0::2,'r' + date_returns]
    else:
        fractile_correlation = df.groupby(by='fractiles')[['s' + date_scores, 'r' + date_returns]].corr().ix[0::2,'r' + date_returns]
    fractile_correlation = pd.DataFrame(list(fractile_correlation))
    fractile_correlation.columns = [date_scores]
    return fractile_correlation.T


def fractile_returns_df(scores, returns, fractile, nmon):
    for i in range(1, len(scores.columns)):
        date = datetime.strptime(scores.columns[i],'%Y%m%d') + relativedelta(months=nmon)
        returns_date = datetime.strftime(datetime(date.year, date.month, calendar.monthrange(date.year, date.month)[1], 0, 0),'%Y%m%d')
        scores_loc = scores.loc[:, ['id', scores.columns[i]]]
        returns_loc = returns.loc[:, ['id', returns_date]]
        if (i==1): fractile_returns_df = fractile_returns(scores_loc, returns_loc, fractile)
        else: fractile_returns_df = pd.concat([fractile_returns_df, fractile_returns(scores_loc, returns_loc, fractile)])
    return fractile_returns_df


def fractile_correlation_df(scores, returns, sector, fractile, nmon, by_sector):
    for i in range(1, len(scores.columns)):
        date = datetime.strptime(scores.columns[i],'%Y%m%d') + relativedelta(months=nmon)
        returns_date = datetime.strftime(datetime(date.year, date.month, calendar.monthrange(date.year, date.month)[1], 0, 0),'%Y%m%d')
        scores_loc = scores.loc[:, ['id', scores.columns[i]]]
        scores_loc = pd.merge(scores_loc,sector[['issuer_id','gics_sector']],left_on='id',right_on='issuer_id',how='left')
        returns_loc = returns.loc[:, ['id', returns_date]]
        if (i==1): fractile_correlation_df = fractile_correlation(scores_loc, returns_loc, fractile, by_sector)
        else: fractile_correlation_df = pd.concat([fractile_correlation_df, fractile_correlation(scores_loc, returns_loc, fractile, by_sector)])
    return fractile_correlation_df

def signal_test_write_returns(scores,returns,nmon,file,open):
    quintile_returns  = signal_test_tools.fractile_returns_df(scores, returns, 5,nmon)
    decile_returns =  signal_test_tools.fractile_returns_df(scores, returns, 10,nmon)
    quintile_returns.columns=[['q1','q2','q3','q4','q5']]
    decile_returns.columns = [['d1','d2','d3','d4','d5','d6','d7','d8','d9','d10']]
    fractile_returns_write = pd.merge(quintile_returns, decile_returns, on=None,left_index=True, right_index=True, how='outer')
    basic_tools.write_to_sheet(fractile_returns_write, file, 'returns', open)

def signal_test_write_ic(scores,returns,sector,nmon,file,open):
    for k in range(1,13):
        if (k==1): universe_correlation = signal_test_tools.fractile_correlation_df(scores, returns, sector, 1, k+nmon-1, False)
        else: universe_correlation = pd.merge(universe_correlation, signal_test_tools.fractile_correlation_df(scores, returns, sector, 1, k+nmon-1, False), on=None, left_index=True, right_index=True, how='outer')
    quintile_correlation  = signal_test_tools.fractile_correlation_df(scores, returns,  sector, 5,nmon, False)
    universe_correlation.columns=[['IC1','IC2','IC3','IC4','IC5','IC6','IC7','IC8','IC9','IC10','IC11','IC12']]
    quintile_correlation.columns=[['q1','q2','q3','q4','q5']]
    scores_loc = scores.loc[:, ['id', scores.columns[1]]]
    scores_loc = pd.merge(scores_loc, sector[['issuer_id', 'gics_sector']], left_on='id', right_on='issuer_id', how='left')
    x = list(scores_loc.gics_sector.unique())
    x.sort()
    sector_correlation = signal_test_tools.fractile_correlation_df(scores, returns, sector, 1, nmon, True)
    sector_correlation.columns = x
    correlation_write = pd.merge(universe_correlation, quintile_correlation, on=None,left_index=True, right_index=True, how='outer')
    correlation_write = pd.merge(correlation_write, sector_correlation, on=None,left_index=True, right_index=True, how='outer')
    basic_tools.write_to_sheet(correlation_write, file, 'IC', open)

