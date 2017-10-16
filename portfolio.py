class Portfolio:
    dates = []
    ids = []
    weights = []
    holdings = []
    return_usd = []
    return_local = []
    portfolio_return_usd = []
    portfolio_return_local = []

    def __init__(self, weights, value):
        self.dates = weights.index
        self.ids = list(weights)
        self.weights = weights.div(weights.sum(axis=1), axis=0)
        self.value = value
        self.holdings = weights * value
        self.calc_portfolio_return()

    def calc_portfolio_return(self):
        import clean_data_tools as cdt
        import pandas as pd
        new_dates = self.dates.union([self.dates[-1]+1])     # Add one period to dates
        self.return_usd = cdt.get_clean_data('ReturnUsd', new_dates, self.ids)
        self.return_local = cdt.get_clean_data('ReturnLocal', new_dates, self.ids)
        lagged_weights = self.weights.reindex(new_dates).shift(1).reindex(new_dates)
        self.portfolio_return_usd = (lagged_weights * self.return_usd).sum(axis=1)
        self.portfolio_return_local = (lagged_weights * self.return_local).sum(axis=1)


