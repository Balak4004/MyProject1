
import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import pytest
import logging
from TestConfig.config import *

logging.basicConfig(
    filename = r'H:\pythonprojects\MyProject1\TestLogs\Tests.log',
    filemode = "a",
    format = "%(asctime)s - %(levelname)s - %(message)s",
    level = logging.INFO
)
logger = logging.getLogger(__name__)

# create mysql engine
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')



# verify data between file as source and database as target
def file_to_db_verify1(file_path, file_type, table_name, db_engine):
    if file_type == 'csv':
        df_src = pd.read_csv(file_path)
    else:
        raise ValueError(f"unsupported file type: {file_type}")
    #logger.info(f"source data from file: {df_src}")
    query = f"""select * from {table_name}"""
    df_tgt = pd.read_sql(query, db_engine)
    #logger.info(f"target data from table: {df_tgt}")

    logger.info("comparing data between src and tgt ")
    concat_df = pd.concat([df_src, df_tgt])
    diff_src_tgt_df = concat_df.drop_duplicates(keep=False)
    #logger.info(f"mismatch data between src and tgt {diff_src_tgt_df} ")

    if not diff_src_tgt_df.empty:
        diff_file_path = r'H:\pythonprojects\MyProject1\TestOutput\diff_src_tgt_xl.xlsx'
        #diff_src_tgt_df.to_csv(diff_file_path, index=False)
        with pd.ExcelWriter(diff_file_path) as writer:
            diff_src_tgt_df.to_excel(writer, sheet_name='Sheet1', index=False)
            diff_src_tgt_df.to_excel(writer, sheet_name='Sheet2', index=False)
        logger.info(f"Differences saved to {diff_file_path}")
    else:
        logger.info("no differences")

    assert df_src.equals(df_tgt), f"Data mismatch between src and TGT: {table_name}"




