import db_tools
import basic_tools
import pandas as pd


def get_raw_data(item_name, daterange, ids):
    # figure out the item_id & table_name
    sql_references = get_sql_references(item_name)

    # Get start_date, end_date, item_id and table_name
    from_date = daterange[0]
    to_date = daterange[-1]
    table_name = sql_references[0]
    item_id = sql_references[1]
    if type(ids) is not list: ids = list([ids])

    # Run the SQL query to load the fundamental data
    if table_name == 'fundamental':
        from_date = from_date - pd.DateOffset(years=2)  # Go back 2 years for fundamental data for interpolation
        periodicity = 1
        fill_limit = 730     # Forward fill for 2 years as we only have annual data
        data = db_tools.get_fundamental_data(ids, item_id, periodicity, from_date, to_date, db_tools.connect_db())

    elif table_name == 'market':
        periodicity = 365
        fill_limit = 5      # Forward fill for 5 days to cover any holidays or missing data
        data = db_tools.get_market_data(ids, item_id, periodicity, from_date, to_date, db_tools.connect_db())

    elif table_name == 'estimate':
        periodicity = 1
        fill_limit = 0
        data = db_tools.get_estimate_data(ids, item_id, periodicity, from_date, to_date, db_tools.connect_db())
    else:
        raise ValueError('Known table name in sql database')

    # Clean and convert to data frame
    data['dates'] = pd.to_datetime(data['dates'], format="%Y%m%d")
    data.ids = data.ids.astype(int)
    data.numeric_value = data.numeric_value.astype(float)
    data = data.drop_duplicates(['dates', 'ids'], keep='last')
    data = data.pivot(index='dates', columns='ids', values='numeric_value')
    data = data.reindex(index=data.index.union([from_date]).union([to_date]))  # Make sure data frame is bounded
    data = basic_tools.convert_to(data, 'd')
    data = data.ffill(limit=fill_limit)

    # Reindex back to requested dates and ids
    data = basic_tools.convert_to(data, daterange.freqstr[0])
    data = data.reindex(index=daterange, columns=ids)
    return data


def get_sql_references(item_name):
    reference_table = {
        #   item_id                    table_name              item_id
        'TotalAssets': ['fundamental', '2'],
        'CurrentAssets': ['fundamental', '3'],
        'Capex': ['fundamental', '8'],
        'Cash': ['fundamental', '9'],
        'CashAndShortTerm': ['fundamental', '10'],
        'Cogs': ['fundamental', '11'],
        'CommonEquity': ['fundamental', '13'],
        'RetainedEarnings': ['fundamental', '17'],
        'TotalDebt': ['fundamental', '19'],
        'LongTermDebt': ['fundamental', '21'],
        'ShortTermDebt': ['fundamental', '26'],
        'Depreciation': ['fundamental', '28'],
        'EBIT': ['fundamental', '41'],
        'EBITDA': ['fundamnetal', '42'],
        'EPS': ['fundamental', '43'],
        'TotalEquity': ['fundamental', '46'],
        'TotalExpenses': ['fundamental', '47'],
        'GrossIncome': ['fundamental', '54'],
        'TotalLiabilities': ['fundamental', '69'],
        'CurrentLiabilities': ['fundamental', '70'],
        'NetDebt': ['fundamental', '81'],
        'NetIncome': ['fundamental', '82'],
        'Sales': ['fundamental', '103'],
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