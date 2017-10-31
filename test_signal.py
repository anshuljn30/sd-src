import clean_data_tools as cdt
import db_tools as db
import signal_test_tools as stt
import pandas as pd
import numpy as np
import basic_tools as bt
import signals as s
from shutil import copyfile

dir = "C:\Investment_research\\"
open = True
nmon = 1

sector = pd.read_csv("C:/Investment_research/issuer_master_sector.csv",sep=',')

dates = pd.date_range('20051231','20161231', freq='m')

ids = db.get_all_security_ids()

stt.run_signal_test('ebit2mktcap',dates,ids)

sector = pd.read_csv("C:/Investment_research/issuer_master_sector.csv",sep=',')


#ids = bt.get_universe(dates)

e2p = s.e2p(dates,ids)

signal_name = 'ebit2mkt_cap'
ebit2mkt_cap = s.ebit2mktcap(dates,ids)


signal = ebit2mkt_cap.T
returns = returns.T

src = dir + 'signal_test_template.xlsx'
output = dir + 'signal_test_' + signal_name + '.xlsx'
copyfile(src, output)

stt.run_signal_test(signal,returns,sector,nmon,output,open)


