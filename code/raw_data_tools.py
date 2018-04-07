import db_tools
import basic_tools
import pandas as pd
import numpy as np


def get_raw_data(item_name, date_range, ids, **kwargs):
    # figure out the item_id & table_name
    sql_references = get_sql_references(item_name)

    # Get start_date, end_date, item_id and table_name
    from_date = date_range[0]
    to_date = date_range[-1]
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

        data, fill_limit = get_raw_fundamental_data(ids, item_id, freq_type, date_range, db_tools.connect_db())

    elif table_name == 'market':
        periodicity = 365
        fill_limit = 5      # Forward fill for 5 days to cover any holidays or missing data
        data = db_tools.get_market_data(ids, item_id, periodicity, from_date, to_date, db_tools.connect_db())

    elif table_name == 'estimate':
        periodicity = 1
        fill_limit = 45
        if 'estimate_type' in kwargs:
            estimate_type = kwargs['estimate_type']
        else:
            estimate_type = 'mean'

        if 'forward_year' in kwargs:
            forward_year = kwargs['forward_year']
        else:
            forward_year = 1

        data = db_tools.get_estimate_data(ids, item_id, periodicity, from_date, to_date, estimate_type, forward_year, db_tools.connect_db())

    else:
        raise ValueError('Unknown table name in sql database')

    data = convert_to_df(data, date_range, ids, fill_limit)
    return data


def convert_to_df(data, date_range, ids, fill_limit):
    # Clean and convert to data frame
    data = data.pivot(index='dates', columns='ids', values='numeric_value')
    data = data.reindex(index=data.index.union([date_range[0]]).union([date_range[-1]]))  # Make sure data frame is bounded
    data = basic_tools.convert_to(data, 'd')
    data = data.ffill(limit=fill_limit)

    # Reindex back to requested dates and ids
    data = basic_tools.convert_to(data, date_range.freqstr[0])
    data = data.reindex(index=date_range, columns=ids)
    return data


def get_raw_fundamental_data(ids, item_id, freq_type, date_range, db):
    from_date = date_range[0] - pd.DateOffset(years=2)  # Go back 2 years for fundamental data for interpolation
    to_date = date_range[-1]

    annual_fill_limit = 500
    quarterly_fill_limit = 150

    if freq_type.lower() == 'annual':
        fill_limit = annual_fill_limit
        data = db_tools.get_fundamental_data(ids, item_id, 1, from_date, to_date, db)
    elif freq_type.lower() == 'quarterly':
        fill_limit = quarterly_fill_limit
        data = db_tools.get_fundamental_data(ids, item_id, 4, from_date, to_date, db)
    elif freq_type.lower() == 'ltm':
        fill_limit = quarterly_fill_limit
        data = get_ltm(ids, item_id, date_range, quarterly_fill_limit, annual_fill_limit, db)
    elif freq_type.lower() == 'ntm':
        fill_limit = quarterly_fill_limit
        data = get_ntm(ids, item_id, from_date, to_date, db)
    else:
        raise ValueError('Frequency type %s not supported.', freq_type)

    return data, fill_limit


def get_ltm(ids, item_id, date_range, quarterly_fill_limit, annual_fill_limit, db):
    from_date = date_range[0]
    to_date = date_range[-1]
    quarterly_data = db_tools.get_fundamental_data(ids, item_id, 4, from_date, to_date, db)
    if any(quarterly_data['fundamental_statement_type'] == 'Balance Sheet'):
        raise ValueError('This is a Balance Sheet item. Please request either Quarterly or Annual data.')

    quarterly_data = quarterly_data.dropna(subset=['period_end_date'])
    quarterly_data_fiscal = quarterly_data.pivot(index='period_end_date', columns='ids', values='numeric_value')
    quarterly_data_fiscal = basic_tools.convert_to(quarterly_data_fiscal, 'Q')

    # Compute LTM and reset the dates
    quarterly_data_ltm = quarterly_data_fiscal.rolling(window=4).sum()
    quarterly_data_ltm = quarterly_data_ltm.unstack()
    quarterly_data_ltm.name = 'numeric_value'
    quarterly_data_ltm = quarterly_data_ltm.reset_index()
    quarterly_data_ltm = quarterly_data_ltm.merge(quarterly_data[['dates', 'ids', 'period_end_date']], on=['ids', 'period_end_date'], how='left')
    quarterly_data_ltm = quarterly_data_ltm.dropna(axis=0, subset=['dates'], how='all')
    quarterly_data_ltm = convert_to_df(quarterly_data_ltm, date_range, ids, quarterly_fill_limit)

    # Get annual data & fill
    annual_data = db_tools.get_fundamental_data(ids, item_id, 1, from_date, to_date, db)
    annual_data = convert_to_df(annual_data, date_range, ids, annual_fill_limit)
    quarterly_data_ltm[quarterly_data_ltm.isnull()] = annual_data
    data = quarterly_data_ltm.unstack()
    data = data.reset_index()
    data.columns = ['ids', 'dates', 'numeric_value']
    return data


def get_ntm(ids, item_id, from_date, to_date, db):
    fy1_data = db_tools.get_estimate_data(ids, item_id, 1, from_date, to_date, 'mean', 1, db)
    fy2_data = db_tools.get_estimate_data(ids, item_id, 1, from_date, to_date, 'mean', 2, db)

    fy1_data = fy1_data.rename(columns={'period_end_date': 'fy1_period_end_date', 'numeric_value': 'fy1_value'})
    fy2_data = fy2_data.rename(columns={'period_end_date': 'fy2_period_end_date', 'numeric_value': 'fy2_value'})

    data = pd.merge(fy1_data, fy2_data, how='outer', on=['dates', 'ids'])
    data['no_months_to_fy1'] = (data['fy1_period_end_date'] - data['dates']) / pd.Timedelta(1, 'M')
    data['no_months_to_fy1'] = (np.ceil(data['no_months_to_fy1'])).clip(0, None)
    data['numeric_value'] = (1/12)*(data['no_months_to_fy1']*data['fy1_value'] + (12-data['no_months_to_fy1'])*data['fy2_value'])

    data = data[['dates', 'ids', 'numeric_value']]
    return data


def get_sql_references(item_name):
    if item_name.startswith('estimated_'):
        actual_item_name = item_name[10:]
    else:
        actual_item_name = item_name

    reference_table = {
        #   item_id                    table_name              item_id
        'total_assets': ['fundamental', '2', 'Balance Sheet'],
        'current_assets': ['fundamental', '3', 'Balance Sheet'],
        'capex': ['fundamental', '8', 'Cash Flow'],
        'cash': ['fundamental', '9', 'Balance Sheet'],
        'cash_and_short_term': ['fundamental', '10', 'Balance Sheet'],
        'cogs': ['fundamental', '11', 'Income Statement'],
        'common_equity': ['fundamental', '13', 'Balance Sheet'],
        'retained_earnings': ['fundamental', '17', 'Balance Sheet'],
        'total_debt': ['fundamental', '19', 'Balance Sheet'],
        'long_term_debt': ['fundamental', '21', 'Balance Sheet'],
        'short_term_debt': ['fundamental', '26', 'Balance Sheet'],
        'depreciation': ['fundamental', '28', 'Balance Sheet'],
        'ebit': ['fundamental', '41', 'Income Statement'],
        'ebitda': ['fundamnetal', '42', 'Income Statement'],
        'eps': ['fundamental', '43', 'Income Statement'],
        'total_equity': ['fundamental', '46', 'Balance Sheet'],
        'total_expenses': ['fundamental', '47', 'Income Statement'],
        'gross_income': ['fundamental', '54', 'Income Statement'],
        'total_liabilities': ['fundamental', '69', 'Balance Sheet'],
        'current_liabilities': ['fundamental', '70', 'Balance Sheet'],
        'net_debt': ['fundamental', '81', 'Balance Sheet'],
        'net_income': ['fundamental', '82', 'Income Statement'],
        'sales': ['fundamental', '103', 'Income Statement'],
        'price_local_unadj': ['market', '1'],
        'price_local_adj': ['market', '2'],
        'price_usd_unadj': ['market', '3'],
        'price_usd_adj': ['market', '4'],
        'shares_outstanding_unadj': ['market', '5'],
        'shares_outstanding_adj': ['market', '6'],
        'company_mcap_local': ['market', '7'],
        'company_mcap_usd': ['market', '8'],
        'security_mcap_local': ['market', '9'],
        'security_mcap_usd': ['market', '10'],
        'volume_shares_unadj': ['market', '11'],
        'volume_shares_adj': ['market', '12']
    }

    references = reference_table[actual_item_name]
    if item_name.startswith('estimated_'):
        references[0] = 'estimate'

    return references

