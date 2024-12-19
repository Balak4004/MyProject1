# Test Extraction from JSON and validate against target ( .csv file )
import pandas as pd
import pytest
from sqlalchemy import create_engine

'''
def get_difference(df_src, df_tgt,file_path):
    concat_df = pd.concat([df_src, df_tgt])
    diff_src_tgt_df = concat_df.drop_duplicates(keep=False)

    if not diff_src_tgt_df.empty:
        diff_src_tgt_df.to_csv(file_path, index=False)
    return diff_src_tgt_df
    '''

mysql_engine = create_engine('mysql+pymysql://root:admin%402024@localhost:3306/retaildwh')

@pytest.fixture()
def csv_file_path():
    return pd.read_csv(r'H:\pythonprojects\MyProject1\TestData\sales_data_dupl.csv')

@pytest.fixture()
def tbl_sales_data():
    query = """select * from staging_sales"""
    return pd.read_sql(query, mysql_engine)


def test_dept_data_compare(csv_file_path, tbl_sales_data):
    diff_file_path = r'H:\pythonprojects\MyProject1\TestOutput\diff_src_tgt.csv'
    concat_df =  pd.concat([csv_file_path, tbl_sales_data])
    diff_src_tgt_df =concat_df.drop_duplicates(keep=False)
    if not diff_src_tgt_df.empty:
        diff_src_tgt_df.to_csv(diff_file_path, index=False)
        #return diff_src_tgt_df
    #diff_src_tgt_df = get_difference(csv_file_path, tbl_sales_data, diff_file_path)

    assert csv_file_path.equals(tbl_sales_data), "dept json and csv not matching"

