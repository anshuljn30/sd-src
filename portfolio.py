class Portfolio:
    dates = []
    ids = []
    weights = []
    holdings = []
    portfolio_return = []

    def __init__(self, weights, value):
        self.dates = weights.index
        self.ids = weights.columns
        self.weights = weights.div(weights.sum(axis=1), axis=0)
        self.value = value
        self.holdings = weights * value
        self.calc_portfolio_return()

    def calc_portfolio_return(self):
        import clean_data_tools as cdt
        return_usd = cdt.get_clean_data('ReturnUsd', self.dates, self.ids)
        return_local = cdt.get_clean_data('ReturnLocal', self.dates, self.ids)
        self.portfolio_return_usd = (self.weights.shift(1) * return_usd).sum(axis=1)
        self.portfolio_return_local = (self.weights.shift(1) * return_local).sum(axis=1)


