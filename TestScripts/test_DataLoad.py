import pandas as pd
from sqlalchemy import create_engine
import pytest
import logging

from CommonUtilities.Utilities import mysql_to_mysql_load_verify
from TestConfig.config import *

logging.basicConfig(
    filename=r'H:\pythonprojects\MyProject1\TestLogs\Tests.log',
    filemode='a',
    format = '%(asctime)s - %(levelname)s - %(message)s',
    level = logging.INFO
)
logger = logging.getLogger(__name__)

# create mysql engine
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

def test_load_fact_sales():
    logger.info("Data comparison started for fact_sales")
    try:
        query1 = """SELECT sales_id, product_id, store_id, quantity, total_sales, sale_date
                    FROM fact_sales order by sales_id, product_id, store_id;"""
        query2 = """SELECT sales_id, product_id, store_id, quantity, total_amount as total_sales, sale_date
                    FROM sales_with_details order by sales_id, product_id, store_id;"""
        mysql_to_mysql_load_verify(query1, mysql_engine, query2, mysql_engine)
        logger.info("Data comparison complete for fact_sales")
    except Exception as e:
        logger.error(f"Data mismatch for fact_sales {e}")
        pytest.fail(f"Test case failed due to fact_sales data mismatch {e}")

def test_load_inventory_fact():
    logger.info("Data comparison started for fact_inventory")
    try:
        query1 = """select * from fact_inventory order by product_id, store_id;"""
        query2 = """select * from staging_inventory order by product_id, store_id;"""
        mysql_to_mysql_load_verify(query1, mysql_engine, query2, mysql_engine)
        logger.info("Data comparison complete for fact_inventory")
    except Exception as e:
        logger.error(f"Data mismatch for fact_inventory {e}")
        pytest.fail(f"Test case failed due to fact_inventory data mismatch {e}")

@pytest.mark.xfail
def test_load_inventory_levels_by_store():
    logger.info("Data comparison started for inventory_levels_by_store")
    try:
        query1 = """select store_id,total_inventory from inventory_levels_by_store 
                where store_id IS NOT NULL order by store_id;"""
        query2 = """select store_id,total_inventory from aggregated_inventory_levels order by store_id;"""
        mysql_to_mysql_load_verify(query1, mysql_engine, query2, mysql_engine)
        logger.info("Data comparison complete for inventory_levels_by_store")
    except Exception as e:
        logger.error(f"Data mismatch for inventory_levels_by_store {e}")
        pytest.fail(f"Test case failed due to inventory_levels_by_store data mismatch {e}")

@pytest.mark.skip
def test_load_monthly_Sales_summary():
    logger.info("Data comparison started for monthly_sales_summary")
    try:
        query1 = """select * from monthly_sales_summary order by product_id, month, year;"""
        query2 = """select * from monthly_sales_summary_source order by product_id, month, year;"""
        mysql_to_mysql_load_verify(query1, mysql_engine, query2, mysql_engine)
        logger.info("Data comparison complete for monthly_sales_summary")
    except Exception as e:
        logger.error(f"Data mismatch for monthly_sales_summary {e}")
        pytest.fail(f"Test case failed due to monthly_sales_summary data mismatch {e}")

