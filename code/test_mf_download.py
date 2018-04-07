import pandas as pd
import download_canara_data as canara

dates = pd.date_range('20121001', '20171031', freq='m')
canara.download(dates)