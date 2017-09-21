import db_tools
import basic_tools

issuer_id = "'1','2'"
item_id=10
periodicity=1
from_date=20050101
to_date=20100101

df=db_tools.get_fundamental_data(issuer_id,item_id,periodicity,from_date,to_date,db_tools.connect_db())
print(df)

df['start_date'] = basic_tools.eom(df['start_date'],"%Y%m%d")
print (df)

df = df.pivot(index='issuer_id',columns='start_date',values='numeric_value')
print (df)