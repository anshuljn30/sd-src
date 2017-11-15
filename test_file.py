import clean_data_tools as cdt
import pandas as pd
import basic_tools as bt
import portfolio as p

dates = pd.date_range('20000131', '20161130', freq='m')
#univ = bt.get_universe(dates)
ids = list(range(1,600))
eps = cdt.get_clean_data('eps', dates=dates, ids=ids)
eps2 = cdt.get_clean_data('estimated_eps', universe=univ)

cap = cdt.get_clean_data('CompanyMcapUsd', dates=dates, ids=ids)
eps_sector = bt.aggregate(eps, 'sector', 'mean')

print(eps_sector)

import company as c
companies = c.Company(ids)
sectors = companies.Sector


eps = cdt.get_clean_data('TotalAssetsUsd', dates=dates, ids=ids)