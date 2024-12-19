import pandas as pd
from sqlalchemy import create_engine, text
import logging
import cx_Oracle
import sys

# Add project directory to sys.path for module imports
sys.path.append(r'H:\pythonprojects\MyProject1')

logging.basicConfig(
    filename = r'H:\pythonprojects\MyProject1\logs\extraction.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level = logging.INFO)
logger = logging.getLogger(__name__)

# Create mysql engine
from scripts.config import *
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

# Create oracle engine
# oracle_engine = create_engine('oracle+cx_oracle://hr:hr@localhost:1521/xe')
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

def extract_sales_dataSRC_loadSTG():
    try:
        logger.info("Data extraction started from sales csv")
        df = pd.read_csv(r'H:\pythonprojects\MyProject1\Data\sales_data.csv')
        logger.info("sales csv file loaded successfully")
        df.to_sql("stg_sales_data", mysql_engine, if_exists='replace', index=False)
        logger.info("Data loaded to stg_sales_data in mysql")
    except Exception as e:
        logger.error(f"an error occurred in stg_sales_data loading")

def extract_product_dataSRC_loadSTG():
    try:
        logger.info("Data extraction started from product csv")
        df = pd.read_csv(r"H:\pythonprojects\MyProject1\Data\product_data.csv")
        logger.info("product csv file loaded successfully")
        df.to_sql("stg_product_data", mysql_engine, if_exists='replace', index=False)
        logger.info("Data loaded to stg_product_data in mysql")
    except Exception as e:
        logger.error(f"error in stg_product_data loading: {e}")

def extract_supplier_dataSRC_loadSTG():
    try:
        logger.info("Data extraction started from supplier json")
        df = pd.read_json(r"H:\pythonprojects\MyProject1\Data\supplier_data.json")
        logger.info("supplier json file loaded successfully")
        df.to_sql("stg_supplier", mysql_engine, if_exists='replace', index=False)
        logger.info("Data loaded to stg_supplier")
    except Exception as e:
        logger.error(f"error in supplier data loading: {e}")

def extract_inventory_dataSRC_loadSTG():
    try:
        logger.info("Data extraction started from inventory xml")
        df = pd.read_xml(r"H:\pythonprojects\MyProject1\Data\inventory_data.xml", xpath=".//item")
        logger.info(r"inventory xml file data loaded")
        df.to_sql("stg_inventory_data", mysql_engine, if_exists='replace', index=False)
        logger.info("data loaded to stg_inventory")
    except Exception as e:
        logger.error(f"error in inventory data loading: {e}")

def extract_stores_dataSRC_loadSTG():
    try:
        query = """select * from stores"""
        df = pd.read_sql(query,oracle_engine)
        logger.info("data extracted from oracle stores")
        df.to_sql("stg_stores", mysql_engine, if_exists='replace', index=False)
        logger.info("data loaded to stg_stores in mysql")
    except Exception as e:
        logger.error(f"error in data loading to stg_stores: {e}")

if __name__ == '__main__':
    logger.info("script execution started")
    try:
        extract_sales_dataSRC_loadSTG()
        extract_product_dataSRC_loadSTG()
        extract_inventory_dataSRC_loadSTG()
        extract_supplier_dataSRC_loadSTG()
        extract_stores_dataSRC_loadSTG()
        logger.info("Data extraction completed")
    except Exception as e:
        logger.error(f"an error occurred sales csv extraction: {e}")
    logger.info("script execution completed")