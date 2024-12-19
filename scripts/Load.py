from stat import filemode
import pandas as pd
from sqlalchemy import create_engine, text
import cx_Oracle
import logging

logging.basicConfig(
    filename = 'H:\pythonprojects\MyProject1\logs\etlprocessproject.log', #path of log file
    filemode = 'a',   # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO) # Set the logging level
logger = logging.getLogger(__name__)




# Create mysql engine
from scripts.config import *

mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

def load_fact_sales():
    query = text("""INSERT INTO fact_sales 
        (sales_id, product_id, store_id, quantity, total_sales, sale_date)
        SELECT sales_id, product_id, store_id, quantity, total_amount, sale_date
        FROM sales_with_details;""")
    try:
        with mysql_engine.connect() as conn:
            logger.info("Executing load_fact_sales function to load the fact_sales table")
            conn.execute(query)
            conn.commit()
        logger.info("Data load completed for fact_sales")
    except Exception as e:
        logger.error(f"An error occurred in fact_sales:  {e}")

def load_inventory_fact():
    query = text("""insert into fact_inventory 1""")
    try:
        with mysql_engine.connect() as conn:
            logger.info("Executing load_inventory_fact function to load the fact_inventory")
            conn.execute(query)
            conn.commit()
        logger.info("Data load completed for inventory_fact")
    except Exception as e:
        logger.error(f"An error occurred in inventory_fact: {e}")

def load_inventory_levels_by_store():
    query = text("""insert into inventory_levels_by_store(store_id,total_inventory) 
            select store_id,total_inventory from aggregated_inventory_levels;""")
    try:
        with mysql_engine.connect() as conn:
            logger.info("Executing load_inventory_levels_by_store function to load the inventory_levels_by_store")
            conn.execute(query)
            conn.commit()
        logger.info("Data load completed for inventory_levels_by_store")
    except Exception as e:
        logger.error(f"An error occurred in inventory_levels_by_store: {e}")

def load_monthly_Sales_summary():
    query = text("""insert into monthly_sales_summary select * from monthly_sales_summary_source;""")
    try:
        with mysql_engine.connect() as conn:
            logger.info("Executing load_monthly_summary function to load monthly_summary ")
            conn.execute(query)
            conn.commit()
        logger.info("Data load completed for monthly_summary")
    except Exception as e:
        logger.info(f"An error occured in monthly_summary: {e}")


if __name__ == '__main__':
    print("Data load started....")
    load_fact_sales()
    load_inventory_fact()
    load_inventory_levels_by_store()
    load_monthly_Sales_summary()
    print("Data load completed.......")