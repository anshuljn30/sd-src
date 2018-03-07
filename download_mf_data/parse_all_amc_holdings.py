import pandas as pd
from download_mf_data import parse_mf_holdings as mff


dates = pd.date_range('20140228', '20171231', freq='m')
amc_names = [
             # Already Parsed
             'baroda_pioneer', 'canara_robeco', 'bnp_paribas', 'boi_axa', 'edelweiss', 'franklin_templeton',
             'idbi', 'idfc', 'indiabulls', 'mahindra', 'mirae', 'ppfas', 'shriram', 'tata', 'taurus', 'union',

             # Some Problem with Parsing
             'axis', 'motilal_oswal','principal','reliance',

             # Can't be parsed - missing isins
             'escorts',

             # Yet to be downloaded
             'birla_sun_life', 'dhfl_paramerica', 'dsp_blackrock', 'essel', 'hdfc', 'hsbc', 'icici_prudential',
             'iifl', 'invesco', 'jm_financial', 'kotak', 'lic', 'lnt', 'quantum', 'sahara', 'sbi', 'srei',
             'sundaram', 'uti']

for i in range(12, 16):
    mff.parse_files(amc_names[i], dates)
