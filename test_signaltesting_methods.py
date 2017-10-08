import pandas as pd
import numpy as np
import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta
import signal_test_tools
import basic_tools

open = True
nmon = 1
scores = pd.read_csv("C:/Investment_research/scores.csv", sep=',')
returns = pd.read_csv("C:/Investment_research/returns.csv", sep=',')
sector = pd.read_csv("C:/Investment_research/data/data_base_static/issuer_master_sector.csv",sep=',')
file = "C:\Investment_research\signal_test.xlsx"

signal_test_tools.signal_test_write_returns(scores,returns,nmon,file,False)

signal_test_tools.signal_test_write_ic(scores,returns,sector,nmon,file,False)

signal_test_tools.signal_test_write_coverage_turnover(scores,sector,5,True,file,open)

