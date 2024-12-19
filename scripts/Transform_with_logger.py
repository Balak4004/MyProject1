import pandas as pd
from sqlalchemy import create_engine, text
import logging
import sys

# Add project directory to sys.path for module imports
sys.path.append(r'H:\pythonprojects\MyProject1')

logging.basicConfig(
    filename=r'H:\pythonprojects\MyProject1\logs\transform.log',
    filemode="a",
    format = '%(asctime)s - %(levelname)s - %(message)s',
    level = logging.INFO)
logger = logging.getLogger(__name__)

# Create mysql engine
from scripts.config import *
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

def filter_sales_data():
    try:
        logger.info("Data transformation started for filter_sales_data")
        query = """select * from staging_sales where sale_date <='2024-09-20'"""
        df = pd.read_sql(query, mysql_engine)
        logger.info("Data transformation completed for filter_sales_data")
        df.to_sql("trf_filtered_sales", mysql_engine, if_exists='replace', index=False)
        logger.info("Data loaded to trf_filtered_sales")
    except Exception as e:
        logger.error(f"error occurred in trf_filtered_sales data loading: {e}")

def router_sales_data():
    try:
        logger.info("Data transformation started for router_sales_data_high")
        query = """select * from filtered_sales where region='High'"""
        df = pd.read_sql(query, mysql_engine)
        logger.info("Data transformation complete for router_sales_data_high")
        df.to_sql("trf_high_sales", mysql_engine, if_exists='replace', index=False)
        logger.info("Data loaded to trf_high_sales")
    except Exception as e:
        logger.error(f"error occurred in trf_high_sales data loading: {e}")
    try:
        logger.info("Data transformation started for router_sales_data_low")
        query = """select * from filtered_sales where region='Low'"""
        df = pd.read_sql(query, mysql_engine)
        logger.info("Data transformation complete for router_sales_data_low")
        df.to_sql("trf_low_sales", mysql_engine, if_exists='replace', index=False)
        logger.info("Data loaded to trf_low_sales")
    except Exception as e:
        logger.error(f"error occurred in trf_low_sales data loading: {e}")

def agg_sales_data():
    try:
        logger.info("Data transformation started for agg_sales_data")
        query = """select product_id,month(sale_date) as month,year(sale_date) as year ,sum(quantity*price) as total_sales from filtered_sales
               group by product_id,month(sale_date),year(sale_date)"""
        df = pd.read_sql(query, mysql_engine)
        logger.info("Data transformation complete for agg_sales_data")
        df.to_sql("trf_monthly_sales_summary_src", mysql_engine, if_exists='replace', index=False)
        logger.info("Data loaded to trf_monthly_sales_summary_src")
    except Exception as e:
        logger.error(f"error occurred in trf_monthly_sales_summary_src data loading: {e}")

def join_sales_data():
    try:
        logger.info("Data transformation started for join_sales_data")
        query = """select s.sales_id,s.product_id,s.store_id,p.product_name,st.store_name,s.quantity,
                s.price*s.quantity as total_amount,s.sale_date
                from filtered_sales as s
                join staging_product as p on s.product_id = p.product_id
                join staging_stores as st on s.store_id = st.store_id"""
        df = pd.read_sql(query, mysql_engine)
        logger.info("Data transformation complete for join_sales_data")
        df.to_sql("trf_sales_details", mysql_engine, if_exists='replace', index=False)
        logger.info("Data loaded to trf_sales_details")
    except Exception as e:
        logger.error(f"error occurred in trf_sales_details data loading: {e}")

def agg_inventory_levels():
    try:
        logger.info("data transformation started for agg_inventory_levels")
        query = """select store_id,sum(quantity_on_hand) as total_inventory 
                from staging_inventory group by store_id"""
        df = pd.read_sql(query, mysql_engine)
        logger.info("data transformation complete for agg_inventory_levels")
        df.to_sql("trf_agg_inventory_levels", mysql_engine, if_exists='replace', index=False)
        logger.info("data loaded to trf_agg_inventory_levels")
    except Exception as e:
        logger.error(f"error occurred in trf_agg_inventory_levels data loading: {e}")

if __name__ == '__main__':
    try:
        logger.info("script execution started")
        filter_sales_data()
        router_sales_data()
        agg_sales_data()
        join_sales_data()
        agg_inventory_levels()
        logger.info("script execution complete")
    except Exception as e:
        logger.error(f"error in script execution: {e}")



