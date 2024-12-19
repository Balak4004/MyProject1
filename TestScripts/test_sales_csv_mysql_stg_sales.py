import pandas as pd
import logging
import pytest
from sqlalchemy import create_engine

logging.basicConfig(
    filename=r'H:\pythonprojects\MyProject1\TestLogs\Tests.log',
    filemode='a',
    format = '%(asctime)s - %(levelname)s - %(message)s',
    level = logging.INFO
)
logger  = logging.getLogger(__name__)

mysql_engine = create_engine('mysql+pymysql://root:admin%402024@localhost:3306/retaildwh')

def test_csv_mysql_sales_data():
    logger.info("csv data loading started")
    try:
        df_src = pd.read_csv(r'H:\pythonprojects\MyProject1\TestData\sales_data_dupl.csv')
        #logger.info(f"csv data {df_src}")
        logger.info("csv data loading completed")
    except Exception as e:
        logger.error("error in csv data load")

    logger.info("mysql table data load started")
    try:
        query = """select * from staging_sales"""
        df_tgt = pd.read_sql(query, mysql_engine)
        #logger.info(f"mysql data {df_tgt}")
        logger.info("mysql table data load completed")
    except Exception as e:
        logger.error("error in mysql table data load")

    logger.info("Comparing CSV and MySQL data")
    concat_df = pd.concat([df_src, df_tgt])
    diff_src_tgt_df = concat_df.drop_duplicates(keep=False)

    if not diff_src_tgt_df.empty:
        diff_file_path = r'H:\pythonprojects\MyProject1\TestOutput\diff_src_tgt2.csv'
        diff_src_tgt_df.to_csv(diff_file_path, index=False)
        logger.info(f"Differences saved to {diff_file_path}")
    else:
        logger.info("No differences found between CSV and MySQL data")

    logger.info("data validation started")
    try:
        assert df_src.equals(df_tgt), " Data not matching"
        logger.info("data validation completed")
    except Exception as e:
        logger.error(f"error in data validation: {e}")
        pytest.fail(f"data mismatch: {e}")



