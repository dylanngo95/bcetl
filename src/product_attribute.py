import datetime
import os
import dotenv
import luigi
import pandas as pd
import pyodbc


class ProductAttribute(luigi.Task):

    # Load environment
    dotenv.load_dotenv()
    MSSQL_SERVER = os.getenv('MSSQL_SERVER')
    DATABASE = os.getenv('DATABASE')
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    MSSQL_DRIVER = os.getenv('MSSQL_DRIVER')

    # Create a new connection
    connectionString = f'DRIVER={MSSQL_DRIVER};SERVER={MSSQL_SERVER};DATABASE={DATABASE};UID={DB_USERNAME};PWD={DB_PASSWORD}'
    conn = pyodbc.connect(connectionString)
    cursor = conn.cursor()

    def output(self):
        return luigi.LocalTarget('log/product_attribute.txt')

    def run(self):
        with self.output().open('w') as f:
            f.write('{date} : Start Export Product Attribute\n'.format(date=datetime.datetime.now()))

        # Incremental export
        SQL_QUERY_PRODUCT_ATTRIBUTE = """
        SELECT
            pratt_map.No_ as productId,
            pratt.[Name] as name,
            pratt_va.[Value] as value,
            pratt_va.[Numeric Value] as numbericValue
        FROM
            [Demo Database BC (23-0)].dbo.[CRONUS USA, Inc_$Item Attribute Value Mapping$437dbf0e-84ff-417a-965d-ed2bb9650972] pratt_map
        LEFT JOIN [Demo Database BC (23-0)].dbo.[CRONUS USA, Inc_$Item Attribute$437dbf0e-84ff-417a-965d-ed2bb9650972] pratt ON
            pratt_map.[Item Attribute ID] = pratt.[ID]
        LEFT JOIN [Demo Database BC (23-0)].dbo.[CRONUS USA, Inc_$Item Attribute Value$437dbf0e-84ff-417a-965d-ed2bb9650972] pratt_va ON
            pratt_map.[Item Attribute Value ID] = pratt_va.[Id]
        ORDER BY  pratt_map.No_
        """

        df_product_attribute = pd.read_sql(SQL_QUERY_PRODUCT_ATTRIBUTE, self.conn)

        now = datetime.datetime.now()
        dateTime = now.strftime('%Y%m%d_%S')
        df_product_attribute.to_csv('output/product_attribute_{}.csv'.format(dateTime),
                  sep='|',
                  escapechar='\r',
                  encoding='utf-8',
                  quotechar='"',
                  index=False
                  )
