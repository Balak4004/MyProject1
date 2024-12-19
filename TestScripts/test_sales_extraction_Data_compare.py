import pandas as pd
from sqlalchemy import create_engine
import pytest
import logging
import cx_Oracle

from CommonUtilities.Utility_sales_mismatch_Capture import file_to_db_verify1
from TestConfig.config import *

#create mysql engine
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

logging.basicConfig(
    filename =r'H:\pythonprojects\MyProject1\TestLogs\Tests.log',
    filemode = 'a',
    format = '%(asctime)s - %(levelname)s - %(message)s',
    level = logging.INFO
)
logger = logging.getLogger(__name__)



def test_extraction_sales_data_csvsrc_to_sales_stg_mysql():
    logger.info("Data comparison started for sales_data")
    try:
        file_to_db_verify1(r'H:\pythonprojects\MyProject1\TestData\sales_data_dupl.csv', 'csv', 'staging_sales', mysql_engine)
        logger.info("Data comparison completed for sales_data")
    except Exception as e:
        logger.error(f"Data mismatch for sales_data: {e}")
        pytest.fail(f"Test failed due to sales_data mismatch {e}")

