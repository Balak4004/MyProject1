import pandas as pd
from sqlalchemy import create_engine
import pytest
import logging
import cx_Oracle

from CommonUtilities.Utilities import file_to_db_verify, oracle_to_mysql_verify
from TestConfig.config import *

#create mysql engine
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

# create oracle engine
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

logging.basicConfig(
    filename =r'H:\pythonprojects\MyProject1\TestLogs\Tests.log',
    filemode = "a",
    format = '%(asctime)s - %(levelname)s - %(message)s',
    level = logging.INFO
)
logger = logging.getLogger(__name__)


def test_extraction_sales_data_csvsrc_to_sales_stg_mysql():
    logger.info("Data comparison started for sales_data")
    try:
        file_to_db_verify(r'H:\pythonprojects\MyProject1\TestData\sales_data.csv', 'csv', 'staging_sales', mysql_engine)
        logger.info("Data comparison completed for sales_data")
    except Exception as e:
        logger.error(f"Data mismatch for sales_data: {e}")
        pytest.fail(f"Test failed due to sales_data mismatch {e}")


def test_extraction_product_data_csvsrc_to_product_stg_mysql():
    logger.info("Data comparison started for product_data")
    try:
        file_to_db_verify(r'H:\pythonprojects\MyProject1\TestData\product_data.csv', 'csv', 'staging_product', mysql_engine)
        logger.info("Data comparison completed for product_data")
    except Exception as e:
        logger.error(f"Data mismatch for product_data: {e}")
        pytest.fail(f"Test failed due to product_data mismatch {e}")

def test_extraction_supplier_data_jsonsrc_to_supplier_stg_mysql():
    logger.info("Data comparison started for supplier_data")
    try:
        file_to_db_verify(r'H:\pythonprojects\MyProject1\TestData\supplier_data.json', 'json', 'staging_supplier', mysql_engine)
        logger.info("Data comparison completed for supplier_data")
    except Exception as e:
        logger.error(f"Data mismatch for supplier_data: {e}")
        pytest.fail(f"Test failed due to supplier_data mismatch {e}")

def test_extraction_inventory_data_xmlsrc_to_inventory_stg_mysql():
    logger.info("Data comparison started for inventory_data")
    try:
        file_to_db_verify(r'H:\pythonprojects\MyProject1\TestData\inventory_data.xml', 'xml', 'staging_inventory', mysql_engine)
        logger.info("Data comparison completed for inventory_data")
    except Exception as e:
        logger.error(f"Data mismatch for inventory_data: {e}")
        pytest.fail(f"Test failed due to inventory_data mismatch {e}")

def test_extraction_stores_data_oraclesrc_to_stores_stg_mysql():
    logger.info("Data comparison started for stores_data")
    try:
        query1 = """select * from stores"""
        query2 = """select * from staging_stores"""
        oracle_to_mysql_verify(query1, oracle_engine, query2, mysql_engine)
        logger.info("Data comparison completed for stores_data")
    except Exception as e:
        logger.error(f"Data mismatch for stores_data: {e}")
        pytest.fail(f"Test failed due to stores_data mismatch {e}")

'''
#def test_extraction_sales_data_src_to_sales_stg_mysql():
    df_src = pd.read_csv(r'H:\pythonprojects\MyProject1\TestData\sales_data.csv')
    query = """select * from staging_sales"""
    df_tgt = pd.read_sql(query, mysql_engine)
    assert df_src.equals(df_tgt), "data mismatch between sales csv and stg sales table"

def test_extraction_product_data_src_to_product_stg_mysql():
    df_src = pd.read_csv(r"H:\pythonprojects\MyProject1\TestData\product_data.csv")
    query = """select * from staging_product"""
    df_tgt = pd.read_sql(query, mysql_engine)
    assert df_src.equals(df_tgt), "Data mismatch between product csv and staging sales table"   '''