

import db_tools as db
import signal_test_tools as stt
import pandas as pd
import signals as s
import basic_tools as bt
import clean_data_tools as cdt




dates = pd.date_range('20051231','20161231', freq='m')
ids = db.get_all_security_ids()
universe = bt.get_universe(dates)

eps = cdt.get_clean_data('EPS', universe=universe)
eps = cdt.get_clean_data('EPS',dates=dates, ids=ids)
price = cdt.get_clean_data('PriceLocalAdj',dates=dates, ids=ids)
e2p = eps / price

e2p = s.e2p(dates, ids)
stt.run_signal_test(e2p,dates,ids)

ebit2mktcap = s.ebit2mktcap(dates, ids)
stt.run_signal_test('ebit2mktcap',dates,ids)


sector = c.Company.sector(ids)


