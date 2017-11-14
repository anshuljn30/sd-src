import clean_data_tools as cdt
import pandas as pd
import basic_tools as bt
import portfolio as p

dates = pd.date_range('20051231', '20141130', freq='m')
#univ = bt.get_universe(dates)
ids = list(range(1,10))
eps = cdt.get_clean_data('EPS', dates=dates, ids=ids)
#eps2 = cdt.get_clean_data('TotalAssets', universe=univ)

cap = cdt.get_clean_data('CompanyMcapUsd', dates=dates, ids=ids)
eps_sector = bt.aggregate(eps, 'sector', 'weighted_mean', weights=cap)
print(eps_sector)
