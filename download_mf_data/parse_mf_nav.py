import pandas as pd
import numpy as np


def parse_files(dates):

    # Read all the master files
    root_path = "C:\\Users\\Administrator\\Documents\\sd-src\\sql_data\\"
    xls_file_path = root_path + "mutual_fund_data\\raw_nav\\"
    xls_out_path = root_path + "mutual_fund_data\\formatted_nav\\"
    xls_out_file = xls_out_path + "all_mf_nav_" + dates[0].strftime['%d-b-%Y'] + ' - ' + dates[-1].strftime['%d-%b-%Y']

    df_all_dates = pd.DataFrame()
    for date in dates:
        date = date.strftime('%d-%b-%Y')
        xls_file_name = 'all_mf_nav_' + date
        xls_file = xls_file_path + xls_file_name + '.xlsx'

        try:
            df = pd.read_csv(xls_file, sep=';')

        except FileNotFoundError:
            print('No File found for date ' + date)
            continue

        print('Parsing NAV data for ' + date)
        df.index = range(len(df.index))
        # Remove any spaces from the fields
        df.replace('^\s+', '', regex=True, inplace=True)  # front
        df.replace('\s+$', '', regex=True, inplace=True)  # end

        # Scrape fund type and amc name and put them in separate column
        where_desc = df['Scheme Name'].isnull()
        where_fund_type = where_desc & df.loc[:, 'Scheme Code'].str.startswith('Open Ended')
        where_amc_name = where_desc & ~where_fund_type

        df_parsed = df
        df_temp = df['Scheme Code']
        df_parsed['fund_type'] = df_temp[where_fund_type].reindex(df_temp.index, method='ffill')
        df_parsed['amc_name'] = df_temp[where_amc_name].reindex(df_temp.index, method='ffill')
        df_parsed = df_parsed[~where_fund_type & ~where_amc_name]
        df_parsed.replace('N.A.', np.NaN)
        df_parsed = df_parsed.drop_duplicates().reset_index(drop=True)

        # Combine all the files together
        if df_parsed.empty:
            print('Nothing Found!')
        else:
            print('Success!')
            df_all_dates = pd.concat([df_all_dates, df_parsed], ignore_index=True)
        del df, df_parsed

    # Writing this clean df to a formatted file
    with open(xls_out_file, 'a') as f:
        print('Writing NAV file for all dates...')
        df_all_dates.to_csv(f, header=False, index=False)