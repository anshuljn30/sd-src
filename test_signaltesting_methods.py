import pandas as pd
import numpy as np
import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta
import signal_test_tools
import basic_tools
from shutil import copyfile


open = True
nmon = 1

scores = pd.read_csv("C:/Investment_research/scores.csv", index_col='ids')
returns = pd.read_csv("C:/Investment_research/returns.csv", index_col='ids')
sector = pd.read_csv("C:/Investment_research/issuer_master_sector.csv",sep=',')
src = "C:\Investment_research\signal_test_template.xlsx"
file = "C:\Investment_research\signal_test.xlsx"
copyfile(src, file)


scores.columns = pd.to_datetime(scores.columns)
returns.columns = pd.to_datetime(returns.columns)

#scores1 = basic_tools.convert_to(scores,'d')
#returns = basic_tools.convert_to(returns,'m')

signal_test_tools.signal_test_write_returns(scores,returns,nmon,file,False)

signal_test_tools.signal_test_write_ic(scores,returns,sector,nmon,file,False)

signal_test_tools.signal_test_write_coverage_turnover(scores,sector,5,True,file,open)

