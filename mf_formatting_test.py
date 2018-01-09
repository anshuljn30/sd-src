
import mutual_fund_formating as mff
import pandas as pd

dates = pd.date_range('20130131', '20171031', freq='m')
mff.parse_files('birla_sun_life', dates)
