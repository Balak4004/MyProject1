from logging import basicConfig

import pandas as pd
from sqlalchemy import create_engine
import logging
import pytest

from CommonUtilities.Count_Utility import oracle_to_count_mysql_verify
from TestConfig.config import *

logging.basicConfig(
    filename =r'H:\pythonprojects\MyProject1\TestLogs\Tests.log', #path of log file
    filemode = 'a',   # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO) # Set the logging level
logger = logging.getLogger(__name__)

# create mysql engine
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

def test_count_filtered_sales_valid():
    logger.info("Data count comparison started for filtered_sales")
    try:
        query1 = """select count(*) from staging_sales where sale_date <='2024-09-20'"""
        query2 = """select count(*) from filtered_sales"""
        oracle_to_count_mysql_verify(query1, mysql_engine, query2, mysql_engine)
        logger.info("Data count comparison completed for filtered_sales")
    except Exception as e:
        logger.error(f"data count mismatch for filtered_sales: {e}")
        pytest.fail(f"Test failed due to filtered_sales count mismatch: {e} ")

def test_count_router_sales_high():
    logger.info("Data count comparison started for high_sales")
    try:
        query1 = """select count(*) from filtered_sales where region='High'"""
        query2 = """select count(*) from high_sales"""
        oracle_to_count_mysql_verify(query1, mysql_engine, query2, mysql_engine)
        logger.info("Data count comparison completed for high_sales")
    except Exception as e:
        logger.error(f"data count mismatch for high_sales: {e}")
        pytest.fail(f"Test failed due to high_sales count mismatch: {e} ")

def test_count_router_sales_low():
    logger.info("Data count comparison started for low_sales")
    try:
        query1 = """select count(*) from filtered_sales where region='Low'"""
        query2 = """select count(*) from low_sales"""
        oracle_to_count_mysql_verify(query1, mysql_engine, query2, mysql_engine)
        logger.info("Data count comparison completed for low_sales")
    except Exception as e:
        logger.error(f"data count mismatch for low_sales: {e}")
        pytest.fail(f"Test failed due to low_sales count mismatch: {e} ")

def test_count_agg_sales_data():
    logger.info("Data count comparison started for monthly_sales_summary_source")
    try:
        query1 = """select count(*) from (
                select product_id,month(sale_date) as month,year(sale_date) as year,
                sum(quantity*price) as total_sales from filtered_sales
               group by product_id,month(sale_date),year(sale_date)
               )s;"""
        query2 = """select count(*) from monthly_sales_summary_source"""
        oracle_to_count_mysql_verify(query1, mysql_engine, query2, mysql_engine)
        logger.info("Data count comparison completed for monthly_sales_summary_source")
    except Exception as e:
        logger.error(f"data count mismatch for monthly_sales_summary_source: {e}")
        pytest.fail(f"Test failed due to monthly_sales_summary_source count mismatch: {e} ")

def test_count_join_sales_data():
    logger.info("Data count comparison started for sales_with_details")
    try:
        query1 = """select count(*) from (
                select s.sales_id,s.product_id,s.store_id,p.product_name,st.store_name,s.quantity,
                s.price*s.quantity as total_amount,s.sale_date
                from filtered_sales as s
                join staging_product as p on s.product_id = p.product_id
                join staging_stores as st on s.store_id = st.store_id
                  )s;"""
        query2 = """select count(*) from sales_with_details"""
        oracle_to_count_mysql_verify(query1, mysql_engine, query2, mysql_engine)
        logger.info("Data count comparison completed for sales_with_details")
    except Exception as e:
        logger.error(f"data count mismatch for sales_with_details: {e}")
        pytest.fail(f"Test failed due to sales_with_details count mismatch: {e} ")

def test_count_agg_inventory_levels():
    logger.info("Data count comparison started for aggregated_inventory_levels")
    try:
        query1 = """select count(*) from (
                select store_id,sum(quantity_on_hand) as total_inventory 
                from staging_inventory group by store_id
                )s;"""
        query2 = """select count(*) from aggregated_inventory_levels"""
        oracle_to_count_mysql_verify(query1, mysql_engine, query2, mysql_engine)
        logger.info("Data count comparison completed for aggregated_inventory_levels")
    except Exception as e:
        logger.error(f"data count mismatch for aggregated_inventory_levels: {e}")
        pytest.fail(f"Test failed due to aggregated_inventory_levels count mismatch: {e} ")