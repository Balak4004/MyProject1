import pandas as pd
from sqlalchemy import create_engine



mysql_engine = create_engine('mysql+pymysql://root:admin%402024@localhost:3306/retaildwh')

df_src = pd.read_xml(r'H:\pythonprojects\MyProject1\TestData\inventory_data.xml', xpath=".//item")
df_src_cnt = df_src.shape[0]
print(df_src_cnt)

query = """select count(*) from staging_product"""
df_tgt = pd.read_sql(query, mysql_engine)
df_tgt_cnt = df_tgt.iloc[0,0]
print(df_tgt_cnt)