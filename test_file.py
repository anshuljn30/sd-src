import clean_data_tools as cdt
import raw_data_tools as rdt
import pandas as pd
import matplotlib.pyplot as plt
import basic_tools as bt
import portfolio as port

dates = pd.date_range('20140131', '20141231', freq='m')
ids = list(range(1,10))
ret = cdt.get_clean_data('ReturnUsd', dates, ids)
#weights = bt.random_frame(10, 10)
#port = port.Portfolio(weights, 1000)
#print(port.portfolio_return_usd)
