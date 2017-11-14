import clean_data_tools as cdt

def e2p(dates,ids):
    kwargs = {"dates": dates, "ids": ids}
    eps = cdt.get_clean_data('EPS',  **kwargs)
    price = cdt.get_clean_data('PriceLocalAdj', dates=dates, ids=ids)
    e2p = eps / price
    return e2p

def ebit2mktcap(dates,ids):
    ebit = cdt.get_clean_data('EBIT',  dates=dates, ids=ids)
    mkt_cap = cdt.get_clean_data('CompanyMcapLocal',dates=dates, ids=ids)
    ebit2mktcap = ebit/mkt_cap
    return ebit2mktcap


