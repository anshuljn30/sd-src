import pandas as pd
import numpy as np
import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta
import basic_tools
import clean_data_tools as cdt
import signals as s
from shutil import copyfile
import company as c


# Jitter will add noise to the data, to avoid a tie in case of splitting data into fractiles
def jitter(series, noise_reduction=100000000):
    return (np.random.random(len(series))*series.std()/noise_reduction)-(series.std()/(2*noise_reduction))


# this method will return fractile returns per cross section
def fractile_returns(scores, returns, fractile):
    date_scores = scores.columns[0]
    date_returns = returns.columns[0]
    scores['fractiles'] = pd.DataFrame(list(pd.qcut(scores[date_scores] + jitter(scores[date_scores]), fractile, labels=False, retbins=True)[0:1])).T
    df_scores = scores.loc[:, ['fractiles']]
    df = pd.merge(df_scores ,returns ,left_index=True,right_index=True ,how='left').set_index(df_scores.index)

    fractile_returns = pd.DataFrame(list(df.groupby(by='fractiles')[date_returns].mean()))
    fractile_returns.columns = [date_scores]
    return fractile_returns.T


# this method will return fractile correlation per cross section, and also correlation by sector if by_sector is set a True
def fractile_correlation(scores, returns, fractile,by_sector):
    date_scores = scores.columns[0]
    date_returns = returns.columns[0]
    scores['fractiles'] = pd.DataFrame(list(pd.qcut(scores[date_scores] + jitter(scores[date_scores]), fractile, labels=False, retbins=True)[0:1])).T
    df_scores = scores.loc[:, ['fractiles',date_scores,'gics_sector_name']]
    df_scores = df_scores.rename(columns={date_scores: 's' + str(date_scores)})
    df_returns = returns.loc[:,[date_returns]]
    df_returns = df_returns.rename(columns={date_returns: 'r' + str(date_returns)})
    df = pd.merge(df_scores, df_returns, left_index=True, right_index=True, how='left').set_index(df_scores.index)

    if(by_sector==True):
        fractile_correlation = df.groupby(by='gics_sector_name')[['s' + str(date_scores), 'r' + str(date_returns)]].corr().ix[0::2,'r' + str(date_returns)]
    else:
        fractile_correlation = df.groupby(by='fractiles')[['s' + str(date_scores), 'r' + str(date_returns)]].corr().ix[0::2,'r' + str(date_returns)]

    fractile_correlation = pd.DataFrame(list(fractile_correlation))
    fractile_correlation.columns = [date_scores]
    return fractile_correlation.T


# this method will return a data frame of fractile returns when a data frame of scores and returns are passsed.
def fractile_returns_df(scores, returns, fractile, nmon):
    for i in range(0, len(scores.columns)):
        date = scores.columns[i] + relativedelta(months=nmon)
        returns_date = datetime(date.year, date.month, calendar.monthrange(date.year, date.month)[1], 0, 0)
        scores_loc = scores.loc[:, [scores.columns[i]]]

        if (returns_date in returns.columns):
            returns_loc = returns.loc[:, [returns_date]]
        else:
            returns_loc = pd.DataFrame(np.nan, index=returns.index, columns=[returns_date])

        if (scores_loc.count().item() >= 2*fractile  ):
            try:
                fractile_returns_df = pd.concat([fractile_returns_df, fractile_returns(scores_loc, returns_loc, fractile)])
            except Exception:
                fractile_returns_df = fractile_returns(scores_loc, returns_loc, fractile)
    return fractile_returns_df


# this method will return a data frame of fractile correlatons when a data frame of score, returns are passed.
# Also correlation by sector is returned if by_sector is set as True.
def fractile_correlation_df(scores, returns, sector, fractile, nmon, by_sector):
    for i in range(0, len(scores.columns)):
        date = scores.columns[i] + relativedelta(months=nmon)
        returns_date = datetime(date.year, date.month, calendar.monthrange(date.year, date.month)[1], 0, 0)
        scores_loc = scores.loc[:, [scores.columns[i]]]
        scores_loc = pd.merge(scores_loc,sector[['ids','gics_sector_name']],left_index=True,right_on='ids',how='left').set_index(scores_loc.index)

        if (returns_date in returns.columns):
            returns_loc = returns.loc[:, [returns_date]]
        else:
            returns_loc = pd.DataFrame(np.nan, index=returns.index, columns=[returns_date])

        if (scores_loc.count()[0].item() >= 2 * fractile and scores_loc.count()[0].item() >= 20):
            try:
                fractile_correlation_df = pd.concat([fractile_correlation_df, fractile_correlation(scores_loc, returns_loc, fractile, by_sector)])
            except Exception:
                fractile_correlation_df = fractile_correlation(scores_loc, returns_loc, fractile, by_sector)

    return fractile_correlation_df


# This method will return returns data required for the signal testing template
def signal_test_write_returns(scores,returns,nmon,file,open):
    quintile_returns  = fractile_returns_df(scores, returns, 5,nmon)
    decile_returns =  fractile_returns_df(scores, returns, 10,nmon)
    quintile_returns.columns=[['q1','q2','q3','q4','q5']]
    decile_returns.columns = [['d1','d2','d3','d4','d5','d6','d7','d8','d9','d10']]
    fractile_returns_write = pd.merge(quintile_returns, decile_returns, on=None,left_index=True, right_index=True, how='outer')

    returns_calc = pd.DataFrame(index=fractile_returns_write.index)
    returns_calc['universe'] = fractile_returns_write[['q1', 'q2', 'q3', 'q4', 'q5']].mean(axis=1)
    returns_calc['qspread'] = fractile_returns_write['q1'] - fractile_returns_write['q5']
    returns_calc['q1excess'] = fractile_returns_write['q1'] - returns_calc['universe']
    returns_calc['dspread'] = fractile_returns_write['d1'] - fractile_returns_write['d10']
    returns_calc['d1excess'] = fractile_returns_write['d1'] - returns_calc['universe']
    basic_tools.write_to_sheet(fractile_returns_write, file, 'returns', False)
    basic_tools.write_to_sheet(returns_calc, file, 'return_calc', open)


# This method will return correlation data required for signal testing template
def signal_test_write_ic(scores,returns,sector,nmon,file,open):
    for k in range(1,13):
        try:
            universe_correlation = pd.merge(universe_correlation, fractile_correlation_df(scores, returns, sector, 1, k + nmon - 1, False),left_index=True, right_index=True, how='outer')

        except Exception:
            universe_correlation = fractile_correlation_df(scores, returns, sector, 1, k + nmon - 1, False)
    quintile_correlation  = fractile_correlation_df(scores, returns,  sector, 5,nmon, False)
    universe_correlation.columns=[['IC1','IC2','IC3','IC4','IC5','IC6','IC7','IC8','IC9','IC10','IC11','IC12']]
    quintile_correlation.columns=[['q1','q2','q3','q4','q5']]
    scores_loc = scores.loc[:, [scores.columns[0]]]
    scores_loc = pd.merge(scores_loc, sector[['ids', 'gics_sector_name']], left_index=True, right_on='ids', how='left').set_index(scores_loc.index)

    x = list(scores_loc.gics_sector_name.unique())
    x = [x for x in x if str(x) != 'nan']
    x.sort()
    sector_correlation = fractile_correlation_df(scores, returns, sector, 1, nmon, True)
    sector_correlation.columns = x
    correlation_write = pd.merge(universe_correlation, quintile_correlation, on=None,left_index=True, right_index=True, how='outer')
    correlation_write = pd.merge(correlation_write, sector_correlation, on=None,left_index=True, right_index=True, how='outer')

    basic_tools.write_to_sheet(correlation_write, file, 'IC', open)


# This method calculates coverage information
def coverage_data(scores,sector,fractile,by_sector):
    quintile = scores.loc[:,[scores.columns[0]]]
    for i in range(0, len(scores.columns)):
        scores_loc = scores.loc[:,[scores.columns[i]]]
        if (scores_loc.count().item() >= 2 * fractile):
            quintile[scores.columns[i]] = pd.DataFrame(list(pd.qcut(scores_loc[scores.columns[i]],fractile,labels=False,retbins=True)[0:1])).T
    if(by_sector==True):
        quintile = pd.merge(quintile, sector[['ids','gics_sector_name']], left_index=True, right_on = 'ids', how = 'inner')

        x = quintile.groupby(['gics_sector_name']).count().T
        x['coverage']  = x.sum(axis=1)
        cols = list(x.columns)
        cols.insert(0, cols.pop(cols.index('coverage')))
        x = x.reindex(columns=cols)
        return x, quintile
    else: return quintile.count(), quintile


## This method will calculate turnover of the quintiles.
def turnover_quintile(scores,quintile):
    to = pd.DataFrame(columns=[['date','Q1','Q2','Q3','Q4','Q5']])
    for i in range(1,len(scores.columns)):
        date = quintile.columns[i]
        date_prev = quintile.columns[i-1]
        to.at[i - 1, 'date'] = date
        try:
            to.at[i-1,'Q1']= 1- (len(quintile.index[quintile[date]==0].intersection(quintile.index[quintile[date_prev]==0]))/len(quintile.index[quintile[date]==0]))
        except Exception:
            to.at[i - 1, 'Q1'] = np.nan

        try:
            to.at[i - 1, 'Q2'] = 1 - (len(quintile.index[quintile[date] == 1].intersection(quintile.index[quintile[date_prev] == 1])) / len(quintile.index[quintile[date] == 1]))
        except Exception:
            to.at[i - 1, 'Q2'] = np.nan

        try:
            to.at[i - 1, 'Q3'] = 1 - (len(quintile.index[quintile[date] == 2].intersection(quintile.index[quintile[date_prev] == 2])) / len(quintile.index[quintile[date] == 2]))
        except Exception:
            to.at[i - 1, 'Q3'] = np.nan

        try:
            to.at[i - 1, 'Q4'] = 1 - (len(quintile.index[quintile[date] == 3].intersection(quintile.index[quintile[date_prev] == 3])) / len(quintile.index[quintile[date] == 3]))
        except Exception:
            to.at[i - 1, 'Q4'] = np.nan

        try:
            to.at[i - 1, 'Q5'] = 1 - (len(quintile.index[quintile[date] == 4].intersection(quintile.index[quintile[date_prev] == 4])) / len(quintile.index[quintile[date] == 4]))
        except Exception:
            to.at[i - 1, 'Q5'] = np.nan


    return to


# This method is useful for writing coverage and turnover data to signal testing template.
def signal_test_write_coverage_turnover(scores,sector,fractile,by_sector,file,open):
    (coverage,quintile) = coverage_data(scores,sector,fractile,by_sector)
    turnover = turnover_quintile(scores,quintile)
    basic_tools.write_to_sheet(coverage, file, 'coverage', False)
    basic_tools.write_to_sheet(turnover, file, 'TO', open)

def run_signal_test(signal,dir = "C:\Investment_research\\",outfile='output',nmon=1,open=True,*args):
    returns = cdt.get_return_local(signal.index, list(signal))
    scores = zscore_clean(signal, axis=1)
    scores = scores.T
    returns = returns.T
    signal_name = outfile

    src = dir + 'signal_test_template.xlsx'
    outfile = dir + 'signal_test_' + outfile + '.xlsx'
    copyfile(src, outfile)
    Company = c.Company(list(signal))
    sector = Company.sector
    sector.rename(columns={'issuer_id': 'ids'}, inplace=True)

    #sector = pd.read_csv("C:/Investment_research/issuer_master_sector.csv", sep=',')
    basic_tools.write_to_sheet(signal, outfile, signal_name, False)
    signal_test_write_returns(scores,returns,nmon,outfile,False)
    signal_test_write_ic(scores,returns,sector,nmon,outfile,False)
    signal_test_write_coverage_turnover(scores,sector,5,True,outfile,open)

def zscore_clean(df,axis=0):
    df = df.sub(df.mean(axis=axis), axis=abs(axis-1)).div(df.std(axis=axis), axis=abs(axis-1))
    df[(df.abs() > 4)] = np.nan
    df = df.sub(df.mean(axis=axis), axis=abs(axis - 1)).div(df.std(axis=axis), axis=abs(axis - 1))
    df[(df.abs() > 4)] = np.nan
    df = df.sub(df.mean(axis=axis), axis=abs(axis - 1)).div(df.std(axis=axis), axis=abs(axis - 1))
    df[(df.abs() > 4)] = np.nan
    df[(df.abs() > 3)] = 3
    df = df.sub(df.mean(axis=axis), axis=abs(axis - 1)).div(df.std(axis=axis), axis=abs(axis - 1))
    return df

