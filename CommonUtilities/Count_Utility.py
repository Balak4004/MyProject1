import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import pytest
import logging

from TestConfig.config import *

# create mysql engine
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

#create oracle engine
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

logging.basicConfig(
    filename =r'H:\pythonprojects\MyProject1\TestLogs\Tests.log', #path of log file
    filemode = 'a',   # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO) # Set the logging level
logger = logging.getLogger(__name__)


# verify data count between file as source and database as target
def file_to_db_count_verify(file_path, file_type, table_name, db_engine):
    if file_type == 'csv':
        df = pd.read_csv(file_path)
    elif file_type == 'json':
        df = pd.read_json(file_path)
    elif file_type == 'xml':
        df = pd.read_xml(file_path, xpath=".//item")
    else:
        f"unsupported file_type: {file_path}"

    df_src_cnt = len(df)
    logger.info(f"src count: {df_src_cnt} from file: {file_path}")

    query = f"""select * from {table_name}"""
    df = pd.read_sql(query, db_engine)
    df_tgt_cnt = len(df)
    #print(df_tgt_cnt)
    logger.info(f"tgt count: {df_tgt_cnt} from table: {table_name}")

    assert df_src_cnt == df_tgt_cnt, "count mismatch between src and cnt"

def oracle_to_count_mysql_verify(query1, db_engine1, query2, db_engine2):
    df = pd.read_sql(query1, db_engine1)
    df_src_cnt = df.iloc[0,0]
    logger.info(f"tgt count: {df_src_cnt} from table: {query1}")
    #print(df_src_cnt)

    df = pd.read_sql(query2, db_engine2)
    df_tgt_cnt = df.iloc[0,0]
    logger.info(f"tgt count: {df_tgt_cnt} from table: {query2}")
    #print(df_tgt_cnt)
    assert df_src_cnt == df_tgt_cnt, "count mismatch between src and TGT"

