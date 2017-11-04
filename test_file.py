import clean_data_tools as cdt
import pandas as pd
import basic_tools as bt
import portfolio as p

dates = pd.date_range('20051231', '20141130', freq='m')
#ids = list(bt.get_universe(dates))
ids = list(range(1,10))
eps = cdt.get_clean_data('EPS', dates, ids, freq_type='Annual')
eps_ltm = cdt.get_clean_data('EPS', dates, ids, freq_type='LTM')

cap = cdt.get_clean_data('CompanyMcapUsd', dates, ids)
eps_sector = bt.aggregate(eps, 'sector', 'weighted_mean', cap)
print(eps_sector)
