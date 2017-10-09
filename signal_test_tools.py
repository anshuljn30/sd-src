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
    date_scores = scores.columns[0]
    date_returns = returns.columns[0]
    scores['fractiles'] = pd.DataFrame(list(pd.qcut(scores[date_scores] + jitter(scores[date_scores]), fractile, labels=False, retbins=True)[0:1])).T
    df_scores = scores.loc[:, ['fractiles']]
    df = pd.merge(df_scores ,returns ,left_index=True,right_index=True ,how='left')
    fractile_returns = pd.DataFrame(list(df.groupby(by='fractiles')[date_returns].mean()))
    fractile_returns.columns = [date_scores]
    return fractile_returns.T

def fractile_correlation(scores, returns, fractile,by_sector):
    date_scores = scores.columns[0]
    date_returns = returns.columns[0]
    scores['fractiles'] = pd.DataFrame(list(pd.qcut(scores[date_scores] + jitter(scores[date_scores]), fractile, labels=False, retbins=True)[0:1])).T
    df_scores = scores.loc[:, ['fractiles',date_scores,'gics_sector']]
    df_scores = df_scores.rename(columns={date_scores: 's' + str(date_scores)})
    df_returns = returns.loc[:,[date_returns]]
    df_returns = df_returns.rename(columns={date_returns: 'r' + str(date_returns)})
    df = pd.merge(df_scores, df_returns, left_index=True, right_index=True, how='left')
    if(by_sector==True):
        fractile_correlation = df.groupby(by='gics_sector')[['s' + str(date_scores), 'r' + str(date_returns)]].corr().ix[0::2,'r' + str(date_returns)]
    else:
        fractile_correlation = df.groupby(by='fractiles')[['s' + str(date_scores), 'r' + str(date_returns)]].corr().ix[0::2,'r' + str(date_returns)]
    fractile_correlation = pd.DataFrame(list(fractile_correlation))
    fractile_correlation.columns = [date_scores]
    return fractile_correlation.T


def fractile_returns_df(scores, returns, fractile, nmon):
    for i in range(0, len(scores.columns)):
        date = scores.columns[i] + relativedelta(months=nmon)
        returns_date = datetime(date.year, date.month, calendar.monthrange(date.year, date.month)[1], 0, 0)
        scores_loc = scores.loc[:, [scores.columns[i]]]
        returns_loc = returns.loc[:, [returns_date]]
        if (i==0): fractile_returns_df = fractile_returns(scores_loc, returns_loc, fractile)
        else: fractile_returns_df = pd.concat([fractile_returns_df, fractile_returns(scores_loc, returns_loc, fractile)])
    return fractile_returns_df


def fractile_correlation_df(scores, returns, sector, fractile, nmon, by_sector):
    for i in range(0, len(scores.columns)):
        date = scores.columns[i] + relativedelta(months=nmon)
        returns_date = datetime(date.year, date.month, calendar.monthrange(date.year, date.month)[1], 0, 0)
        scores_loc = scores.loc[:, [scores.columns[i]]]
        scores_loc = pd.merge(scores_loc,sector[['ids','gics_sector']],left_index=True,right_on='ids',how='left')
        returns_loc = returns.loc[:, [returns_date]]
        if (i==1): fractile_correlation_df = fractile_correlation(scores_loc, returns_loc, fractile, by_sector)
        else: fractile_correlation_df = pd.concat([fractile_correlation_df, fractile_correlation(scores_loc, returns_loc, fractile, by_sector)])
    return fractile_correlation_df

def signal_test_write_returns(scores,returns,nmon,file,open):
    quintile_returns  = fractile_returns_df(scores, returns, 5,nmon)
    decile_returns =  fractile_returns_df(scores, returns, 10,nmon)
    quintile_returns.columns=[['q1','q2','q3','q4','q5']]
    decile_returns.columns = [['d1','d2','d3','d4','d5','d6','d7','d8','d9','d10']]
    fractile_returns_write = pd.merge(quintile_returns, decile_returns, on=None,left_index=True, right_index=True, how='outer')
    basic_tools.write_to_sheet(fractile_returns_write, file, 'returns', open)

def signal_test_write_ic(scores,returns,sector,nmon,file,open):
    for k in range(1,13):
        if (k==1): universe_correlation = fractile_correlation_df(scores, returns, sector, 1, k+nmon-1, False)
        else: universe_correlation = pd.merge(universe_correlation, fractile_correlation_df(scores, returns, sector, 1, k+nmon-1, False), on=None, left_index=True, right_index=True, how='outer')
    quintile_correlation  = fractile_correlation_df(scores, returns,  sector, 5,nmon, False)
    universe_correlation.columns=[['IC1','IC2','IC3','IC4','IC5','IC6','IC7','IC8','IC9','IC10','IC11','IC12']]
    quintile_correlation.columns=[['q1','q2','q3','q4','q5']]
    scores_loc = scores.loc[:, [scores.columns[1]]]
    scores_loc = pd.merge(scores_loc, sector[['ids', 'gics_sector']], left_index=True, right_on='ids', how='left')
    x = list(scores_loc.gics_sector.unique())
    x.sort()
    sector_correlation = fractile_correlation_df(scores, returns, sector, 1, nmon, True)
    sector_correlation.columns = x
    correlation_write = pd.merge(universe_correlation, quintile_correlation, on=None,left_index=True, right_index=True, how='outer')
    correlation_write = pd.merge(correlation_write, sector_correlation, on=None,left_index=True, right_index=True, how='outer')
    basic_tools.write_to_sheet(correlation_write, file, 'IC', open)

def coverage_data(scores,sector,fractile,by_sector):
    quintile = scores.loc[:,['id']]
    for i in range(1, len(scores.columns)):
        score_loc = scores.loc[:,['id', scores.columns[i]]]
        quintile[scores.columns[i]] = pd.DataFrame(list(pd.qcut(score_loc[scores.columns[i]],fractile,labels=False,retbins=True)[0:1])).T
    if(by_sector==True):
        quintile2 = pd.merge(quintile, sector[['issuer_id','gics_sector']], left_on='id', right_on = 'issuer_id', how = 'inner')
        return quintile2.groupby(['gics_sector']).count().T, quintile
    else: return quintile.count(), quintile

def turnover_quintile(scores,quintile):
    to = pd.DataFrame(columns=[['date','Q1','Q2','Q3','Q4','Q5']])
    for i in range(2,len(scores.columns)):
        date = quintile.columns[i]
        date_prev = quintile.columns[i-1]
        to.at[i - 1, 'date'] = date
        to.at[i-1,'Q1']= 1- (len(quintile.index[quintile[date]==0].intersection(quintile.index[quintile[date_prev]==0]))/len(quintile.index[quintile[date]==0]))
        to.at[i-1,'Q2']= 1- (len(quintile.index[quintile[date]==1].intersection(quintile.index[quintile[date_prev]==1]))/ len(quintile.index[quintile[date] == 1]))
        to.at[i-1,'Q3']= 1 - (len(quintile.index[quintile[date]==2].intersection(quintile.index[quintile[date_prev]==2]))/ len(quintile.index[quintile[date] == 2]))
        to.at[i-1,'Q4']= 1 - (len(quintile.index[quintile[date]==3].intersection(quintile.index[quintile[date_prev]==3]))/ len(quintile.index[quintile[date] == 3]))
        to.at[i-1,'Q5']= 1 - (len(quintile.index[quintile[date]==4].intersection(quintile.index[quintile[date_prev]==4]))/ len(quintile.index[quintile[date] == 4]))
    return to

def signal_test_write_coverage_turnover(scores,sector,fractile,by_sector,file,open):
    (coverage,quintile) = coverage_data(scores,sector,fractile,by_sector)
    turnover = turnover_quintile(scores,quintile)
    basic_tools.write_to_sheet(coverage, file, 'coverage', False)
    basic_tools.write_to_sheet(turnover, file, 'TO', open)
