

import db_tools as db
import signal_test_tools as stt
import pandas as pd
import signals as s


dates = pd.date_range('20051231','20161231', freq='m')
ids = db.get_all_security_ids()

e2p = s.e2p(dates, ids)
stt.run_signal_test(e2p,dates,ids)

ebit2mktcap = s.ebit2mktcap(dates, ids)
stt.run_signal_test('ebit2mktcap',dates,ids)




