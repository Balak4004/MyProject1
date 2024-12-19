from logging import basicConfig

import pandas as pd
from sqlalchemy import create_engine
import pytest
import logging

logging.basicConfig(
    filename=r'H:\pythonprojects\MyProject1\TestLogs\Tests.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level = logging.INFO
)
logger = logging.getLogger(__name__)

mysql_engine = create_engine('mysql+pymysql://root:admin%402024@localhost:3306/retaildwh')

def test_stg_sales_dupl_check():
    query = """select * from staging_sales"""
    df = pd.read_sql(query, mysql_engine)
    df['cnt'] = df.groupby('product_id')['product_id'].transform('count')
    df_dupl = df[df['cnt']>6]
    #print(df_dupl)

    logger.info("checking duplicates")
    if not df_dupl.empty:
        dupl_file_path = r'H:\pythonprojects\MyProject1\TestOutput\diff_src_tgt2.csv'
        df_dupl.to_csv(dupl_file_path, index=False)
        logger.info(f"duplicates saved to diff_src_tgt2.csv: {dupl_file_path}")
    else:
        logger.info("No duplicates found ")

    logger.info("Duplicate validation started")
    try:
        assert df_dupl.empty, f"Duplicates exist: {df_dupl}"
        logger.info("Duplicate validation completed")
    except Exception as e:
        logger.error(f"Duplicates exists {e}")
        pytest.fail("test failed as duplicates exists")




