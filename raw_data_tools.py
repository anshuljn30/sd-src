import db_tools
import basic_tools
import pandas as pd


def get_raw_data(item_name, daterange, ids, **kwargs):
    # figure out the item_id & table_name
    sql_references = get_sql_references(item_name)

    # Get start_date, end_date, item_id and table_name
    from_date = daterange[0]
    to_date = daterange[-1]
    table_name = sql_references[0]
    item_id = sql_references[1]
    if type(ids) is not list:
        ids = list([ids])

    # Run the SQL query to load the fundamental data
    if table_name == 'fundamental':
        fundamental_statement = sql_references[2]
        if 'freq_type' in kwargs:
            freq_type = kwargs['freq_type']
        else:
            if fundamental_statement == 'Balance Sheet':
                freq_type = 'Quarterly'
            else:
                freq_type = 'LTM'

        data, fill_limit = get_raw_fundamental_data(ids, item_id, freq_type, daterange, db_tools.connect_db)

    elif table_name == 'market':
        periodicity = 365
        fill_limit = 5      # Forward fill for 5 days to cover any holidays or missing data
        data = db_tools.get_market_data(ids, item_id, periodicity, from_date, to_date, db_tools.connect_db())

    elif table_name == 'estimate':
        periodicity = 1
        fill_limit = 0
        data = db_tools.get_estimate_data(ids, item_id, periodicity, from_date, to_date, db_tools.connect_db())
    else:
        raise ValueError('Unknown table name in sql database')

    data = convert_to_df(data, daterange, ids, fill_limit)
    return data


def convert_to_df(data, daterange, ids, fill_limit):
    # Clean and convert to data frame
    data = data.pivot(index='dates', columns='ids', values='numeric_value')
    data = data.reindex(index=data.index.union([daterange[0]]).union([daterange[-1]]))  # Make sure data frame is bounded
    data = basic_tools.convert_to(data, 'd')
    data = data.ffill(limit=fill_limit)

    # Reindex back to requested dates and ids
    data = basic_tools.convert_to(data, daterange.freqstr[0])
    data = data.reindex(index=daterange, columns=ids)
    return data


def get_raw_fundamental_data(ids, item_id, freq_type, daterange, db):
    from_date = daterange[0] - pd.DateOffset(years=2)  # Go back 2 years for fundamental data for interpolation
    to_date = daterange[-1]

    annual_fill_limit = 500
    quarterly_fill_limit = 120

    if freq_type == 'Annual':
        fill_limit = annual_fill_limit
        data = db_tools.get_fundamental_data(ids, item_id, 1, from_date, to_date, db_tools.connect_db())
    elif freq_type == 'Quarterly':
        fill_limit = quarterly_fill_limit
        data = db_tools.get_fundamental_data(ids, item_id, 4, from_date, to_date, db_tools.connect_db())
    elif freq_type == 'LTM':
        fill_limit = quarterly_fill_limit  # More than 1 quarter
        data = get_ltm(ids, item_id, daterange, quarterly_fill_limit, annual_fill_limit, db_tools.connect_db())
    elif freq_type == 'NTM':
        fill_limit = quarterly_fill_limit  # More than 1 quarter
        data = get_ntm(ids, item_id, from_date, to_date, db_tools.connect_db())
    else:
        raise ValueError('Frequency type %s not supported.', freq_type)

    return data, fill_limit


def get_ltm(ids, item_id, daterange, quarterly_fill_limit, annual_fill_limit, db):
    from_date = daterange[0]
    to_date=daterange[-1]
    quarterly_data = db_tools.get_fundamental_data(ids, item_id, 4, from_date, to_date, db)
    if any(quarterly_data['fundamental_statement_type'] == 'Balance Sheet'):
        raise ValueError('This is a Balance Sheet item. Please request either Quarterly or Annual data.')

    quarterly_data_fiscal = quarterly_data.pivot(index='period_end_date', columns='ids', values='numeric_value')
    quarterly_data_fiscal = basic_tools.convert_to(quarterly_data_fiscal, 'Q')

    # Compute LTM and reset the dates
    quarterly_data_ltm = quarterly_data_fiscal.rolling(window=4).sum()
    quarterly_data_ltm = quarterly_data_ltm.unstack()
    quarterly_data_ltm.name = 'numeric_value'
    quarterly_data_ltm = quarterly_data_ltm.reset_index()
    quarterly_data_ltm = quarterly_data_ltm.merge(quarterly_data[['dates', 'ids', 'period_end_date']], on=['ids', 'period_end_date'], how='left')
    quarterly_data_ltm = quarterly_data_ltm.dropna(axis=0, subset=['dates'], how='all')
    quarterly_data_ltm = convert_to_df(quarterly_data_ltm, daterange, ids, quarterly_fill_limit)

    # Get annual data & fill
    annual_data = db_tools.get_fundamental_data(ids, item_id, 1, from_date, to_date, db)
    annual_data = convert_to_df(annual_data, daterange, ids, annual_fill_limit)
    quarterly_data_ltm[quarterly_data_ltm.isnull()] = annual_data
    data = quarterly_data_ltm.unstack()
    data = data.reset_index()
    data.columns = ['ids', 'dates', 'numeric_value']
    return data


def get_ntm(ids, item_id, from_date, to_date, db):
    data = 1
    return data


def get_sql_references(item_name):
    reference_table = {
        #   item_id                    table_name              item_id
        'TotalAssets': ['fundamental', '2', 'Balance Sheet'],
        'CurrentAssets': ['fundamental', '3', 'Balance Sheet'],
        'Capex': ['fundamental', '8', 'Cash Flow'],
        'Cash': ['fundamental', '9', 'Balance Sheet'],
        'CashAndShortTerm': ['fundamental', '10', 'Balance Sheet'],
        'Cogs': ['fundamental', '11', 'Income Statement'],
        'CommonEquity': ['fundamental', '13', 'Balance Sheet'],
        'RetainedEarnings': ['fundamental', '17', 'Balance Sheet'],
        'TotalDebt': ['fundamental', '19', 'Balance Sheet'],
        'LongTermDebt': ['fundamental', '21', 'Balance Sheet'],
        'ShortTermDebt': ['fundamental', '26', 'Balance Sheet'],
        'Depreciation': ['fundamental', '28', 'Balance Sheet'],
        'EBIT': ['fundamental', '41', 'Income Statement'],
        'EBITDA': ['fundamnetal', '42', 'Income Statement'],
        'EPS': ['fundamental', '43', 'Income Statement'],
        'TotalEquity': ['fundamental', '46', 'Balance Sheet'],
        'TotalExpenses': ['fundamental', '47', 'Income Statement'],
        'GrossIncome': ['fundamental', '54', 'Income Statement'],
        'TotalLiabilities': ['fundamental', '69', 'Balance Sheet'],
        'CurrentLiabilities': ['fundamental', '70', 'Balance Sheet'],
        'NetDebt': ['fundamental', '81', 'Balance Sheet'],
        'NetIncome': ['fundamental', '82', 'Income Statement'],
        'Sales': ['fundamental', '103', 'Income Statement'],
        'PriceLocalUnadj': ['market', '1'],
        'PriceLocalAdj': ['market', '2'],
        'PriceUsdUnadj': ['market', '3'],
        'PriceUsdAdj': ['market', '4'],
        'SharesOutstandingUnadj': ['market', '5'],
        'SharesOutstandingAdj': ['market', '6'],
        'CompanyMcapLocal': ['market', '7'],
        'CompanyMcapUsd': ['market', '8'],
        'SecurityMcapLocal': ['market', '9'],
        'SecurityMcapUsd': ['market', '10'],
        'VolumeSharesUnadj': ['market', '11'],
        'VolumeSharesAdj': ['market', '12']
    }

    references = reference_table[item_name]
    return references