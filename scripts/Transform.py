import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from scripts.config import *

#create mysql engine
#mysql_engine = create_engine('mysql+pymysql://root:admin%402024@localhost:33306/retaildwh')
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

def filter_sales_data():
    query = """select * from staging_sales where sale_date <='2024-09-20'"""
    df = pd.read_sql(query,mysql_engine)
    df.to_sql("filtered_sales", mysql_engine, if_exists='replace', index=False)

def router_sales_data():
    query_high = """select * from filtered_sales where region='High'"""
    df = pd.read_sql(query_high, mysql_engine)
    df.to_sql("high_sales", mysql_engine,if_exists='replace', index=False)

    query_low = """select * from filtered_sales where region='Low'"""
    df = pd.read_sql(query_low,mysql_engine)
    df.to_sql("low_sales", mysql_engine, if_exists='replace', index=False)

def aggregate_sales_data():
    query = """select product_id,month(sale_date) as month,year(sale_date) as year ,sum(quantity*price) as total_sales from filtered_sales
               group by product_id,month(sale_date),year(sale_date)"""
    df = pd.read_sql(query, mysql_engine)
    df.to_sql("monthly_sales_summary_source", mysql_engine, if_exists='replace', index=False)

def join_sales_data():
    query ="""select s.sales_id,s.product_id,s.store_id,p.product_name,st.store_name,s.quantity,
                s.price*s.quantity as total_amount,s.sale_date
                from filtered_sales as s
                join staging_product as p on s.product_id = p.product_id
                join staging_stores as st on s.store_id = st.store_id"""
    df = pd.read_sql(query, mysql_engine)
    df.to_sql("sales_with_details", mysql_engine, if_exists="replace", index=False)

def aggregate_inventory_levels():
    query = """select store_id,sum(quantity_on_hand) as total_inventory 
                from staging_inventory group by store_id"""
    df = pd.read_sql(query, mysql_engine)
    df.to_sql("aggregated_inventory_levels", mysql_engine, if_exists='replace', index=False)



if __name__ == '__main__':
    print("Data transformation started....")
    filter_sales_data()
    router_sales_data()
    aggregate_sales_data()
    join_sales_data()
    aggregate_inventory_levels()
    print("Data transformation completed.....")