import os
import luigi
import pandas as pd
import pyodbc
import datetime
import dotenv


class ItemMaster(luigi.Task):

    # Load environment
    dotenv.load_dotenv()
    MSSQL_SERVER = os.getenv('MSSQL_SERVER')
    DATABASE = os.getenv('DATABASE')
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    MSSQL_DRIVER = os.getenv('MSSQL_DRIVER')

    x = luigi.IntParameter()
    y = luigi.IntParameter(default=45)

    def output(self):
        return luigi.LocalTarget('log/item_master.txt')

    def run(self):
        # Create a new connection
        connectionString = f'DRIVER={self.MSSQL_DRIVER};SERVER={self.MSSQL_SERVER};DATABASE={self.DATABASE};UID={self.DB_USERNAME};PWD={self.DB_PASSWORD}'
        conn = pyodbc.connect(connectionString)
        cursor = conn.cursor()

        # Incremental export
        SQL_QUERY_ITEM_MASTER = """
        SELECT
            itemmaster.NO_ as productId,
            itemmaster.[Item Category Code] as categoryCode,
            itemmaster.[Description] as shortDescription,
            itemmaster.[Lead Time Calculation] as leadTime,
            itemmaster.[Unit Price] as unitPrice,
            itemmaster.[$systemCreatedAt] as systemCreatedAt,
            itemmaster.[Last DateTime Modified] as lastDateTimeModified,
            itemmaster.[Net Weight] as netWeight,
            itemmaster.[Gross Weight] as grossWeight,
            itemmaster.[Unit Volume] as unitVolume
        FROM
            [Demo Database BC (23-0)].dbo.[CRONUS USA, Inc_$Item$437dbf0e-84ff-417a-965d-ed2bb9650972] itemmaster
        WHERE DATEDIFF(DAY, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()) - 1, 0), itemmaster.[Last DateTime Modified]) > 0
        """

        df_item_master = pd.read_sql(SQL_QUERY_ITEM_MASTER, conn)

        # export all, need to fix the duplicate productId
        SQL_QUERY_SALES_PRICE = """
        SELECT
            salesprice.[Item No_] as productId,
            salesprice.[Sales Type] as salesType,
            salesprice.[Sales Code] as SalesCode,
            salesprice.[Minimum Quantity] as minimumQuantity,
            salesprice.[Unit Price] as salesPrice,
            salesprice.[Starting Date] as startingDate,
            salesprice.[Ending Date] as endingDate
        
        FROM [Demo Database BC (23-0)].dbo.[CRONUS USA, Inc_$Sales Price$437dbf0e-84ff-417a-965d-ed2bb9650972] salesprice
        """

        df_sales_price = pd.read_sql(SQL_QUERY_SALES_PRICE, conn)

        merged_df = pd.merge(
            df_item_master,
            df_sales_price,
            left_on='productId',
            right_on='productId',
            how='left'
        )

        # print(df.head(5))
        now = datetime.datetime.now()
        dateTime = now.strftime('%Y%m%d_%S')
        merged_df.to_csv('output/itemmaster_{}.csv'.format(dateTime),
                  sep='|',
                  escapechar='\r',
                  encoding='utf-8',
                  quotechar='"',
                  index=False
                  )