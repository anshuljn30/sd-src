

import db_tools as db
import signal_test_tools as stt
import pandas as pd
import signals as s
import basic_tools as bt
import clean_data_tools as cdt




dates = pd.date_range('20051231','20161231', freq='m')
ids = db.get_all_security_ids()
universe = bt.get_universe(dates)

eps = cdt.get_clean_data('eps', universe=universe)

price = cdt.get_clean_data('price_local_adj',universe=universe)
e2p = eps / price

stt.run_signal_test(e2p,outfile='e2p')


signal = e2p
dir = "C:\Investment_research\\"
file = "C:\Investment_research\\test.csv"
outfile='output'
nmon=1
open=True