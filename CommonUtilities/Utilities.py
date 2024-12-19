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

#create oracle engine
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

# verify data between file as source and database as target
def file_to_db_verify(file_path, file_type, table_name, db_engine):
    if file_type == 'csv':
        df_src = pd.read_csv(file_path)
    elif file_type == 'json':
        df_src = pd.read_json(file_path)
    elif file_type == 'xml':
        df_src = pd.read_xml(file_path, xpath=".//item")
    else:
        raise ValueError(f"unsupported file type: {file_type}")
    #logger.info(f"source data from file: {df_src}")

    query = f"""select * from {table_name}"""
    df_tgt = pd.read_sql(query, db_engine)
    #logger.info(f"target data from table: {df_tgt}")
    assert df_src.equals(df_tgt), f"Data mismatch between src and TGT: {table_name}"

def oracle_to_mysql_verify(query1, db_engine1, query2, db_engine2) :
    df_src = pd.read_sql(query1, db_engine1)
    #logger.info(f"source data from: {table_name1}")

    df_tgt = pd.read_sql(query2, db_engine2)
    #logger.info(f"target data from: {table_name2}")

    assert df_src.equals(df_tgt), f"Data mismatch between src and TGT: {query2}"

def mysql_to_mysql_verify(query1, db_engine1, query2, db_engine2) :
    df_src = pd.read_sql(query1, db_engine1)
    #logger.info(f"source data from: {table_name1}")

    df_tgt = pd.read_sql(query2, db_engine2)
    #logger.info(f"target data from: {df_src}")

    assert df_src.equals(df_tgt), f"Data mismatch between src and TGT: {query2}"

def mysql_to_mysql_load_verify(query1, db_engine1, query2, db_engine2) :
    df_src = pd.read_sql(query1, db_engine1)
    #logger.info(f"source data from: {df_src}")

    df_tgt = pd.read_sql(query2, db_engine2)
    #logger.info(f"target data from: {df_tgt}")

    assert df_src.astype(str).equals(df_tgt.astype(str)), f"Data mismatch between src and TGT: {query2}"