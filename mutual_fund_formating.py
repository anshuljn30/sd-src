import pandas as pd
import re
import numpy as np
import os


xls_file_path="C:\\Users\\NamAnwiSumAmar\\Documents\\india_asset_allocation\\factset_data\\Mutual Fund Data\\hdfc_portfolios\\"
xls_out_path="C:\\Users\\NamAnwiSumAmar\\Documents\\india_asset_allocation\\factset_data\\Mutual Fund Data\\hdfc_portfolios_formated\\"

#xls_file_name = "birlasunlife_201710.xls"
#xls_file_name = "hdfc_portfolios_201710.xls"

df_all_fund = pd.DataFrame()

security_master_file = "C:\Investment_research\data\data_base_static\security_master.csv"
security_master = pd.read_csv(security_master_file)

for xls_file_name in os.listdir(xls_file_path):
    xls_file = xls_file_path + xls_file_name
    xls = pd.ExcelFile(xls_file)
    df_all  = pd.DataFrame()
    for s in range(0,len(xls.sheet_names)):
       df = xls.parse(xls.sheet_names[s])


       df.index = range(len(df.index))

       #df = pd.read_csv("C:\\backtest\\hdfc.csv")
       df.replace('^\s+', '', regex=True, inplace=True) #front
       df.replace('\s+$', '', regex=True, inplace=True) #end
       df2 = df[df.apply(lambda row: row.astype(str).str.contains('ISIN', case=False).any(), axis=1)]
       header_row = df2[df2.apply(lambda row: row.astype(str).str.contains('quantity', case=False).any(), axis=1)]
       header_row = header_row.apply(lambda x: x.astype(str).str.lower())
       if header_row.shape[0]>0:
           df.columns = header_row.iloc[0]
       else:
           print("no holdings found (isin, quantity) in the file "  + xls_file + " and tab " + xls.sheet_names[s])
           continue

       df_clean = pd.DataFrame(columns = df.columns)

       df_clean["IS_EQUITY"] = np.nan
       get_des = 1
       is_equity_flag = 0
       for i in range(0, len(df.index)):
           if re.match(r'^IN', str(df['isin'].iloc[i])) and len(str(df['isin'].iloc[i])) == 12:
               df_temp = pd.DataFrame(df.ix[i:i])
               if is_equity_flag == 1:
                   df_temp.loc[i, 'IS_EQUITY'] = 1
               else:
                   df_temp.loc[i, 'IS_EQUITY'] = np.nan
               if (get_des == 1):
                   x = list(df.iloc[i - 1]) + list(df.iloc[i - 2]) + list(df.iloc[i - 3]) + list(df.iloc[i - 4])
                   str_x = " ".join(str(x) for x in x)
                   str_x = str_x.upper()
                   get_des = 0
                   if re.search(r'\bEQUITY\b', str_x):
                       df_temp.loc[i, 'IS_EQUITY'] = 1
                       is_equity_flag = 1

               df_clean = df_clean.append(df_temp)
           else:
               get_des = 1
               is_equity_flag = 0

       print (xls.sheet_names[s])

       if 'nan' in df_clean.columns:
           del df_clean['nan']
       df_clean['fund_ticker'] = xls.sheet_names[s]
       df_clean["file_name"] = xls_file_name
       df_clean['provider_id']=3
       df_clean['periodicity']=12
       df_clean['currency_code']='INR'
       if df_all.empty:
           df_all = df_clean
       else:
           df_all = pd.concat([df_all, df_clean], ignore_index=True)

       if df_all_fund.empty:
           df_all_fund = df_clean
       else:
           df_all_fund = pd.concat([df_all_fund, df_clean], ignore_index=True)


    df_all = pd.merge(df_all,security_master[['isin','security_id']],left_on='isin',right_on='isin',how='left')
    df_all['security_id'] = df_all['security_id'].apply(lambda x: int(x) if x == x else "")

    df_all.to_csv(xls_out_path + xls_file_name + "_formated.csv", index=False)

df_all_fund = pd.merge(df_all_fund,security_master[['isin','security_id']],left_on='isin',right_on='isin',how='left')
df_all_fund['security_id'] = df_all_fund['security_id'].apply(lambda x: int(x) if x == x else "")


df_master = df_all_fund[['name of instrument','security_id']]

df_master = df_master[df_master['security_id']!='']
df_master = df_master[pd.isnull(df_master['name of instrument']) == False]

df_master = df_master.drop_duplicates()

del df_all_fund['security_id']

df_all_fund = pd.merge(df_all_fund,df_master,left_on='name of instrument',right_on='name of instrument',how='left')
df_all_fund = df_all_fund.drop_duplicates()

df_all_fund.to_csv(xls_out_path+"hdfc_all.csv",index=False)

## assign is_equity = 1 where security_id is present
## add code to map security ids for name of the instrument column.
## remove duplicate names from security master

