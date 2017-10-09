import db_tools
import basic_tools

#issuer_id = "select distinct issuer_id from issuer_master"
issuer_id = "'1','2','3'"
item_id=10
periodicity=1
from_date=20140101
to_date=20160101


df=db_tools.get_fundamental_data(issuer_id,item_id,periodicity,from_date,to_date,db_tools.connect_db())
df['dates'] = basic_tools.eom(df['dates'],"%Y%m%d")
df = df.drop_duplicates(['ids','dates'],keep='last')
df = df.pivot(index='ids',columns='dates',values='numeric_value')

df = basic_tools.convert_to(df,'m')













