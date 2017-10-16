import clean_data_tools as cdt
import pandas as pd
import basic_tools as bt
import portfolio as p

dates = pd.date_range('20141031', '20141130', freq='d')
#ids = list(bt.get_universe(dates))
ids = list(range(1,10))
eps = cdt.get_clean_data('PriceUsdAdj', dates, ids)
print(eps)