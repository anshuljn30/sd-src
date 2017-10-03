import db_tools
import basic_tools

issuer_id = "select distinct issuer_id from issuer_master"
item_id=10
periodicity=1
from_date=20050101
to_date=20160101


df=db_tools.get_fundamental_data(issuer_id,item_id,periodicity,from_date,to_date,db_tools.connect_db())
df['start_date'] = basic_tools.eom(df['start_date'],"%Y%m%d")
df = df.drop_duplicates(['issuer_id','start_date'],keep='last')
df = df.pivot(index='issuer_id',columns='start_date',values='numeric_value')

df = basic_tools.fill_forward(df,0)










