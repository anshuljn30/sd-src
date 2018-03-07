import pandas as pd
import re
import numpy as np


def parse_files(amc_name, dates):

    # Read all the master files
    root_path = "C:\\Users\\Administrator\\Documents\\sd-src\\sql_data\\"
    amc_name_spaced = ' '.join(amc_name.split('_'))
    xls_file_path = root_path + "mutual_fund_data\\raw\\" + amc_name + "\\"
    xls_out_path = root_path + "mutual_fund_data\\formatted\\"
    security_master_file = root_path + "master_files\\security_master.csv"
    new_security_master_file = root_path + "master_files\\new_security_master.csv"
    new_fi_master_file = root_path + "master_files\\new_fi_master.csv"
    new_fund_master_file = root_path + "master_files\\new_fund_master.csv"

    security_master = pd.read_csv(security_master_file)
    security_master = security_master[['security_id', 'security_name', 'isin']]
    new_security_master = pd.read_csv(new_security_master_file)
    security_master = security_master.append(new_security_master)
    new_fi_master = pd.read_csv(new_fi_master_file, encoding='ISO-8859-1')
    new_fi_master = new_fi_master[['security_id', 'security_name', 'isin']]
    new_fund_master = pd.read_csv(new_fund_master_file)
    new_fund_master = new_fund_master[['fund_id', 'fund_ticker', 'fund_name']]

    # Loop through all the fund files
    df_all_fund = pd.DataFrame()
    fund_index = pd.DataFrame(columns=['fund_id', 'fund_name'])
    for date in dates:
        date = date.strftime('%Y%m')
        xls_file_name = amc_name + '_portfolios_' + date
        xls_file = xls_file_path + xls_file_name + '.xlsx'

        try:
            xls = pd.ExcelFile(xls_file)

        except FileNotFoundError:
            print('No File found for date ' + date)
            continue

        # Loop through all the sheets
        for s in range(0, len(xls.sheet_names)):
            sheet_name = xls.sheet_names[s]
            df = xls.parse(sheet_name, header=None)
            df.index = range(len(df.index))

            # Remove any spaces from the fields
            df.replace('^\s+', '', regex=True, inplace=True)  # front
            df.replace('\s+$', '', regex=True, inplace=True)  # end

            # Guess header_row - where the row contains both isin and name
            contains_isin = df.apply(lambda row: row.astype(str).str.contains('isin', case=False).any(), axis=1)
            contains_name = df.apply(lambda row: row.astype(str).str.contains('name', case=False).any(), axis=1)
            header_index = df.index[contains_isin & contains_name]

            if df.empty:
                print("Neither a fund detail nor an index file: " + xls_file + " and tab " + xls.sheet_names[s])
                continue

            elif len(header_index) == 0:
                # Assume it is a fund index containing fund_id and fund_name
                print('Parsing Sheet ' + sheet_name + ' in File ' + xls_file_name + ' as fund index....', end="")
                df_parsed = parse_fund_index(df, amc_name_spaced)
                if not df_parsed.empty:
                    print('Success!')
                    fund_index = fund_index.append(df_parsed)
                else:
                    print('Failure!')
                del df_parsed, df

            else:
                print('Parsing Data for Sheet ' + sheet_name + ' in File ' + xls_file_name + '...', end="")
                header_index = header_index[0]
                df_parsed = parse_sheet_data(df, date, sheet_name, header_index, amc_name_spaced)

                # Combine all the sheets together
                if df_parsed.empty:
                    print('Nothing Found!')
                else:
                    print('Success!')
                    df_all_fund = pd.concat([df_all_fund, df_parsed], ignore_index=True)
                del df, df_parsed

    # Now start cleaning up the data frame
    df_pass1 = map_to_ids(df_all_fund, security_master, new_fi_master, new_fund_master)
    df_pass2 = cleanup_isin(df_pass1)
    df_pass3 = cleanup_fund_names(df_pass2, fund_index, amc_name_spaced, new_fund_master)

    # Assign new ids for new securities & funds and write to csv
    df_security_master = assign_new_security_ids(df_pass3, security_master)
    df_fi_master = assign_new_fi_ids(df_pass3, new_fi_master)
    df_fund_master = assign_new_fund_ids(df_pass3, new_fund_master, amc_name_spaced)

    if not df_security_master.empty:
        print('Writing ', len(df_security_master.index), ' new equity securities to the security_master...')
        with open(new_security_master_file, 'a') as f:
            df_security_master.to_csv(f, header=False, index=False)
        security_master = security_master.append(df_security_master)

    if not df_fi_master.empty:
        print('Writing ', len(df_fi_master.index), ' new fi securities to the new_fi_master...')
        with open(new_fi_master_file, 'a') as f:
            df_fi_master.to_csv(f, header=False, index=False)
        new_fi_master = new_fi_master.append(df_fi_master[['security_id', 'security_name', 'isin']])

    if not df_fund_master.empty:
        print('Writing ', len(df_fund_master.index), ' new funds to the new_fund_master...')
        with open(new_fund_master_file, 'a') as f:
            df_fund_master.to_csv(f, header=False, index=False)
        new_fund_master = new_fund_master.append(df_fund_master)

    security_master = security_master.append(new_fi_master)
    security_master = security_master.drop_duplicates().reset_index(drop=True)
    new_fund_master = new_fund_master.drop_duplicates().reset_index(drop=True)

    # Finally, Merge All new ids, clean up and write to CSV
    df_pass4 = merge_all_new_ids(df_pass3, security_master, new_fund_master)
    write_to_csv(df_pass4, xls_out_path, amc_name)


def map_to_ids(df, security_master, fi_master, fund_master):

    # Map isin to security_id from the database
    df.replace('^\s+', '', regex=True, inplace=True)  # front
    df.replace('\s+$', '', regex=True, inplace=True)  # end
    df.replace('\s+', ' ', regex=True, inplace=True)  # middle
    df_clean = pd.merge(df, security_master[['isin', 'security_id']], left_on='isin', right_on='isin', how='left')

    # Overwrite is_equity = 1 where security_id is present
    where_not_empty = df_clean[df_clean['security_id'].notnull()].index
    df_clean.loc[where_not_empty, 'is_equity'] = 1

    # Overwrite is_equity = 1 if isin has is_equity = 1 elsewhere
    df_isin_isequity = df_clean[['isin', 'is_equity']]
    df_isin_isequity = df_isin_isequity.groupby('isin').apply(lambda x: 1 if any(x['is_equity'] == 1) else np.nan)
    df_isin_isequity = df_isin_isequity.reset_index()
    df_isin_isequity.columns = np.where(df_isin_isequity.columns == 0, 'is_equity', df_isin_isequity.columns)
    del df_clean['is_equity']
    df_clean = pd.merge(df_clean, df_isin_isequity, left_on='isin', right_on='isin', how='left')

    # Map fixed income ids where is_equity is not 1
    df_fi = df_clean[df_clean['is_equity'] != 1]
    df_equity = df_clean[df_clean['is_equity'] == 1]
    del df_fi['security_id']
    df_fi = pd.merge(df_fi, fi_master[['isin', 'security_id']], left_on='isin', right_on='isin', how='left')
    df_clean = df_equity.append(df_fi)

    # Map fund_id from fund_master
    df_clean = pd.merge(df_clean, fund_master[['fund_name', 'fund_id']], left_on='fund_name', right_on='fund_name',
                        how='left')
    return df_clean


def cleanup_isin(df):
    # Certain equities change isin through history, try to match them by name
    df_equity = df[df['is_equity'] == 1]
    df_fi = df[df['is_equity'] != 1]

    df_clean = normalize_column(df_equity, 'security_name', 'security_id')
    df_clean = df_clean.append(df_fi)
    df_clean = df_clean.drop_duplicates()
    return df_clean


def cleanup_fund_names(df, fund_index, amc_name_spaced, fund_master):
    # Clean up fund names from fund index
    df[['fund_ticker', 'fund_name']] = df[['fund_ticker', 'fund_name']].apply(lambda x: x.str.lower())
    if not fund_index.empty:
        fund_index_temp = fund_index[['fund_ticker', 'fund_name']]
        fund_index_temp = fund_index_temp.apply(lambda x: x.str.lower())
        fund_index_temp = fund_index_temp.drop_duplicates(subset='fund_ticker').reset_index(drop=True)

        # Update fund_name from fund_index
        df.fund_name.update(df.fund_ticker.map(fund_index_temp.set_index('fund_ticker').fund_name))

    # Check if the fund_ticker could be a fund_name (more than 1 word) - if yes replace fund_name by this
    where_fund_name = df.index[df['fund_ticker'].apply(lambda row: len(row.split()) > 1)]
    df.loc[where_fund_name, 'fund_name'] = df.loc[where_fund_name, 'fund_ticker']
    df.loc[where_fund_name, 'fund_ticker'] = np.nan

    # If fund_name is null copy the fund ticker to fund name
    where_null = df.index[df['fund_name'].isnull()]
    df.loc[where_null, 'fund_name'] = df.loc[where_null, 'fund_ticker']
    df.loc[where_null, 'fund_ticker'] = np.nan

    # Prefix amc name in the fund name
    where_not_amc = df.index[~df['fund_name'].str.startswith(amc_name_spaced)]
    df.loc[where_not_amc, 'fund_name'] = amc_name_spaced + ' ' + df.loc[where_not_amc, 'fund_name']

    del df['fund_id']
    df_clean = pd.merge(df, fund_master[['fund_name', 'fund_id']], left_on='fund_name', right_on='fund_name',
                        how='left')
    return df_clean


def assign_new_security_ids(df, new_security_master):
    # Get all the equities where security_id is null and assign them a new security_id
    df_equity = df[(df['is_equity'] == 1) & (df['security_id'].isnull())]
    df_equity = normalize_column(df_equity, 'security_name', 'isin')

    nids = len(df_equity.index)
    if nids > 0:
        last_known_security_id = new_security_master['security_id'].iloc[-1]
        df_equity['security_id'] = pd.Series(range(last_known_security_id + 1, last_known_security_id + nids + 1, 1))
        df_equity = df_equity[['security_id', 'security_name', 'isin']]
    return df_equity


def assign_new_fi_ids(df, new_fi_master):
    # Get all the fixed income securities where security_id is null and assign them a new security_id
    df_fi = df[(df['security_id'].isnull()) & (df['is_equity'] != 1)]
    df_fi = df_fi[['isin', 'security_name', 'coupon', 'rating']]
    df_fi = df_fi.drop_duplicates()

    if 'coupon' in df_fi.columns:
        df_fi = normalize_column(df_fi, 'isin', 'coupon')
    if 'rating' in df_fi.columns:
        df_fi = normalize_column(df_fi, 'isin', 'rating')

    nids = len(df_fi.index)
    if nids > 0:
        last_known_fi_id = new_fi_master['security_id'].iloc[-1]
        df_fi['security_id'] = pd.Series(range(last_known_fi_id + 1, last_known_fi_id + nids + 1, 1))
        df_fi = df_fi[['security_id', 'security_name', 'isin', 'coupon', 'rating']]
    return df_fi


def assign_new_fund_ids(df, new_fund_master, amc_name):
    # Get all new fund_names and assign them fund_ids
    df_fund = df[df['fund_id'].isnull()]
    df_fund = df_fund[['fund_ticker', 'fund_name']]
    df_fund = df_fund.drop_duplicates().reset_index(drop=True)
    df_fund_clean = df_fund[df_fund.notnull().all(1)]
    df_fund_dirty = df_fund[df_fund.isnull().any(1)]
    df_fund_clean = df_fund_clean.drop_duplicates(subset='fund_name').reset_index(drop=True)
    df_fund_dirty = df_fund_dirty.drop_duplicates(subset='fund_name').reset_index(drop=True)
    where_found_name = df_fund_dirty.fund_name.map(df_fund_clean.set_index('fund_name').fund_ticker)
    if where_found_name.notnull:
        df_fund_dirty.fund_ticker.update(where_found_name)
    df_fund_dirty = df_fund_dirty.drop_duplicates().reset_index(drop=True)
    where_ticker_null = df_fund_dirty.index[df_fund_dirty['fund_ticker'].isnull()]
    df_fund_dirty.iloc[where_ticker_null, 0] = df_fund_dirty.iloc[where_ticker_null, 1]
    df_fund = df_fund_clean.append(df_fund_dirty)
    df_fund = df_fund.drop_duplicates().reset_index(drop=True)
    df_fund = normalize_column(df_fund, 'fund_name', 'fund_ticker')

    nids = len(df_fund.index)
    if nids > 0:
        last_known_fund_id = new_fund_master['fund_id'].iloc[-1]
        df_fund['fund_id'] = pd.Series(range(last_known_fund_id + 1, last_known_fund_id + nids + 1, 1))
        df_fund = df_fund[['fund_id', 'fund_ticker', 'fund_name']]
        df_fund['amc_name'] = amc_name
    return df_fund


def merge_all_new_ids(df, security_master, fund_master):
    # Populate df with newly created security_ids, fund_ids
    df_null = df[df['security_id'].isnull()]
    df_notnull = df[df['security_id'].notnull()]
    df_null = df_null.drop(['security_id'], axis=1)
    df_null['isin'] = df_null['isin'].astype(str)
    security_master['isin'] = security_master['isin'].astype(str)
    df_clean = pd.merge(df_null, security_master[['isin', 'security_id']], left_on='isin', right_on='isin', how='left')
    df_clean = df_clean.append(df_notnull)
    df_clean = cleanup_isin(df_clean)

    # Now with fund_ids
    df_null2 = df_clean[df_clean['fund_id'].isnull()]
    df_notnull2 = df_clean[df_clean['fund_id'].notnull()]
    df_null2 = df_null2.drop(['fund_id', 'fund_ticker'], axis=1)
    df_clean2 = pd.merge(df_null2, fund_master[['fund_name', 'fund_id']], left_on='fund_name', right_on='fund_name',
                        how='left')
    df_clean2 = df_clean2.append(df_notnull2)
    return df_clean2


def write_to_csv(df, xls_out_path, amc_name):
    # Finally write it to a csv file
    xls_out_test_file = amc_name + "_all_test.csv"
    print('Writing mutual fund test data with ', len(df.index), ' rows to file ', xls_out_test_file)
    df.to_csv(xls_out_path + xls_out_test_file, index=False)
    df_final = df[['date', 'fund_id', 'security_id', 'quantity', 'market_value', 'weight']].copy()
    df_final.loc[:, 'periodicity_id'] = 12
    df_final.loc[:, 'provider_id'] = 3
    xls_out_file = amc_name + "_all_upload.csv"
    print('Writing mutual fund upload data with ', len(df.index), ' rows to file ', xls_out_file)
    df_final.to_csv(xls_out_path + xls_out_file, index=False)


def parse_sheet_data(df, date, sheet_name, header_index, amc_name_spaced):

    # Find fund name in the sheet - a string contained within (amc_name) and (fund)
    df_head = df.iloc[0:header_index+1]
    is_match = df_head.apply(
        lambda name: re.search('%s(.*)%s' % (amc_name_spaced.lower(), 'fund'), str(name).lower())).dropna()
    if is_match.any():
        fund_name = is_match.any().group(0)
    else:
        fund_name = []

    # Rename columns to make them consistent
    header_row = df.iloc[header_index, :]
    df.columns = header_row
    df.columns = df.columns.astype(str).str.lower()
    df = df.drop([col for col in df.columns if col.strip() == 'nan'], axis=1).copy()
    df = df.loc[:, ~df.columns.duplicated()]
    isin_col = df.filter(regex='isin').columns
    df.rename(columns={isin_col[0]: 'isin'}, inplace=True)
    name_col = df.filter(regex='name').columns
    df.rename(columns={name_col[0]: 'security_name'}, inplace=True)
    weight_col = df.filter(regex='nav|assets|%').columns
    df.rename(columns={weight_col[0]: 'weight'}, inplace=True)
    market_val_col = df.filter(regex='market').columns
    if not market_val_col.empty:
        df.rename(columns={market_val_col[0]: 'market_value'}, inplace=True)
    else:
        df['market_value'] = ""
    quantity_col = df.filter(regex='quantity').columns
    if not quantity_col.empty:
        df.rename(columns={quantity_col[0]: 'quantity'}, inplace=True)
    else:
        df['quantity'] = ""
    rating_col = df.filter(regex='rating').columns
    if rating_col.empty:
        rating_col = df.filter(regex='industry').columns

    if not rating_col.empty:
        df.rename(columns={rating_col[0]: 'rating'}, inplace=True)
    else:
        df['rating'] = ""
    coupon_col = df.filter(regex='coupon').columns
    if not coupon_col.empty:
        df.rename(columns={coupon_col[0]: 'coupon'}, inplace=True)
    else:
        df['coupon'] = ""

    # Loop through each row of the sheet and grab rows where isin is present
    df_clean = pd.DataFrame(columns=df.columns)
    df_clean['is_equity'] = np.nan
    get_des = 1
    is_equity_flag = 0
    counter = 0
    for i in range(0, len(df.index)):
        df_temp = pd.DataFrame(df.iloc[[i]])
        # Check if we find isin
        if any((df_temp['isin'].astype(str).str[:2] == 'IN') & (df_temp['isin'].astype(str).str.len() == 12)):
            counter = 0
            if is_equity_flag == 1:
                df_temp['is_equity'] = 1
            else:
                df_temp['is_equity'] = np.nan

            # Search for the word Equity in the 3 lines preceding the first isin
            if get_des == 1:
                x = list(df.iloc[i - 1]) + list(df.iloc[i - 2]) + list(df.iloc[i - 3]) + list(df.iloc[i - 4])
                str_x = " ".join(str(x) for x in x)
                str_x = str_x.lower()
                get_des = 0
                matches_equity = ((re.search(r'equity', str_x) is not None) |
                    (re.search(r'equities', str_x) is not None) | (re.search(r'preferred', str_x) is not None))
                matches_debt = ((re.search(r'debt', str_x) is not None) | (re.search(r'debenture', str_x) is not None) |
                     (re.search(r'bond', str_x) is not None))
                if matches_debt:
                    is_equity_flag = 0
                else:
                    if matches_equity:
                        df_temp.loc[i, 'is_equity'] = 1
                        is_equity_flag = 1
            df_clean = df_clean.append(df_temp)
            del df_temp

        else:
            if any((df_temp['isin'].isnull()) | (df_temp['isin'] == '')):
                # Check if name is also blank, then only turn the equity flag off
                if any((df_temp['security_name'].isnull()) | (df_temp['security_name'] == '')):
                    is_equity_flag = 0
                    get_des = 1
                else:
                    counter = counter + 1
            else:
                counter = counter + 1

            # If there are 2 blank isin in a row then turn equity flag off
            if counter > 1:
                is_equity_flag = 0
                get_des = 1

    # Clean up data frame - Replace any # , @, $ or * from the beginning and end of the name
    if 'nan' in df_clean.columns:
        del df_clean['nan']

    df_clean['security_name'].replace('^[\s@#*$^-]+', '', regex=True, inplace=True) # front
    df_clean['security_name'].replace('[\s@#*$^-]+$', '', regex=True, inplace=True) # end

    df_clean['fund_ticker'] = sheet_name
    df_clean["date"] = date
    if fund_name:
        df_clean['fund_name'] = fund_name
    else:
        df_clean['fund_name'] = np.nan
    del df
    return df_clean


def parse_fund_index(df, amc_name_spaced):
    is_header_row = df.apply(lambda row: row.astype(str).str.contains('name', case=False).any(), axis=1)
    if is_header_row.any():
        header_row = (df[is_header_row].iloc[0]).astype(str)
        df = df.iloc[df.index[is_header_row][0] + 1:].copy()
        df.columns = header_row.apply(lambda row: row.lower())
        name_col = [col for col in df.columns if 'name' in col]
        name_contains_amc = df[name_col[0]].str.contains(amc_name_spaced, case=False)
        if name_contains_amc.any():
            ticker_col = df.columns[df.columns.get_loc(name_col[0]) - 1]
            df.rename(columns={name_col[0]: 'fund_name'}, inplace=True)
            df.rename(columns={ticker_col: 'fund_ticker'}, inplace=True)
            df = df.dropna()
        else:
            df = pd.DataFrame()
    else:
        df = pd.DataFrame()
    return df


def normalize_column(df, group_col, normal_col):
    df_temp = df[[group_col, normal_col]]
    df_temp = df_temp.dropna(axis=0, how='any')
    df_temp = df_temp.drop_duplicates()
    df_temp = df_temp.groupby(group_col).apply(lambda x: x[normal_col].value_counts().idxmax())
    df_temp = df_temp.reset_index()
    df_temp.columns = np.where(df_temp.columns == 0, normal_col, df_temp.columns)
    del df[normal_col]
    df_clean = pd.merge(df, df_temp, left_on=group_col, right_on=group_col, how='left')
    if not df_clean.empty:
        df_clean = df_clean.drop_duplicates(subset=group_col).reset_index(drop=True)
    return df_clean
