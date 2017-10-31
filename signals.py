import clean_data_tools as cdt

def e2p(dates,ids):
    eps = cdt.get_clean_data('EPS', dates, ids)
    price = cdt.get_clean_data('PriceLocalAdj', dates, ids)
    e2p = eps / price
    return e2p

def ebit2mktcap(dates,ids):
    ebit = cdt.get_clean_data('EBIT', dates, ids)
    mkt_cap = cdt.get_clean_data('CompanyMcapLocal',dates, ids)
    ebit2mktcap = ebit/mkt_cap
    return ebit2mktcap()


