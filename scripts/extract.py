import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle

# Create mysql engine
#mysql_engine = create_engine('mysql+pymysql://root:admin%402024@localhost:3306/retaildwh')
from scripts.config import *

mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

# Create oracle engine
# oracle_engine = create_engine('oracle+cx_oracle://hr:hr@localhost:1521/xe')
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

def extract_sales_dataSRC_Load_STG():
    df = pd.read_csv("H:\pythonprojects\MyProject1\Data\sales_data.csv")
    df.to_sql("staging_sales", mysql_engine, if_exists='replace', index=False)

def extract_product_dataSRC_Load_STG():
    df = pd.read_csv ("H:\pythonprojects\MyProject1\Data\product_data.csv")
    df.to_sql("staging_product", mysql_engine, if_exists='replace', index=False)

def extract_supplier_dataSRC_Load_STG():
    df = pd.read_json("H:\pythonprojects\MyProject1\Data\supplier_data.json")
    df.to_sql("staging_supplier", mysql_engine, if_exists='replace', index=False)

def extract_inventory_dataSRC_Load_STG():
    df = pd.read_xml("H:\pythonprojects\MyProject1\Data\inventory_data.xml", xpath=".//item")
    df.to_sql("staging_inventory", mysql_engine, if_exists='replace', index=False)

def extract_stores_data_oracleSRC_Load_STG():
    query = """select * from stores"""
    df = pd.read_sql(query, oracle_engine)
    df.to_sql("staging_stores", mysql_engine, if_exists='replace', index=False)


if __name__ == '__main__':
    print("Data extraction started....")
    extract_sales_dataSRC_Load_STG()
    extract_product_dataSRC_Load_STG()
    extract_supplier_dataSRC_Load_STG()
    extract_inventory_dataSRC_Load_STG()
    extract_stores_data_oracleSRC_Load_STG()
    print("data extraction completed....")










