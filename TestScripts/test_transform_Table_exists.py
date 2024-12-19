from operator import truediv

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(
    filename = r'H:\pythonprojects\MyProject1\TestLogs\Tests.log',
    filemode = 'a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level = logging.INFO
)
logger=logging.getLogger(__name__)

# Database connection setup
mysql_engine = create_engine('mysql+pymysql://root:admin%402024@localhost:3306/etlqalabsdb')

# List of table names to check
tables_to_check = ['filtered_sales', 'high_sales', 'low_sales', 'monthly_sales_summary_source',
                   'sales_with_details', 'aggregated_inventory_levels']

# Function to check if a table exists
def table_exists1(table_name):
    try:
        query = f"""SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_name = '{table_name}' AND table_schema = 'retaildwh';"""
        with mysql_engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()

        if result and result[0]==1:
            logger.info(f"table: {table_name} exists")
        return True

    except Exception as e:
        logger.error(f"table not exists: {table_name}, {e}")
        return False

# Test function using pytest
def test_table_exists1():
    for table in tables_to_check:
        assert table_exists1(table), f"Table: {table_name} does not exists"




