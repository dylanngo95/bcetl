import os
import luigi
import pandas as pd
import pyodbc
import datetime
import dotenv


class ItemMaster(luigi.Task):
    dotenv.load_dotenv()

    MSSQL_SERVER = os.getenv('MSSQL_SERVER')
    DATABASE = os.getenv('DATABASE')
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    MSSQL_DRIVER = os.getenv('MSSQL_DRIVER')
    x = luigi.IntParameter()
    y = luigi.IntParameter(default=45)

    def run(self):
        # Configuration
        SERVER = self.MSSQL_SERVER
        DATABASE = self.DATABASE
        DB_USERNAME = self.DB_USERNAME
        DB_PASSWORD = self.DB_PASSWORD
        MSSQL_DRIVER = self.MSSQL_DRIVER

        # Create the connection
        connectionString = f'DRIVER={MSSQL_DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={DB_USERNAME};PWD={DB_PASSWORD}'
        conn = pyodbc.connect(connectionString)
        cursor = conn.cursor()

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
        
            
        SELECT * FROM 
            [Demo Database BC (23-0)].dbo.Sale
        """

        # pd.set_option('display.max_columns', None)
        # pd.set_option('display.max_rows', None)
        df_item_master = pd.read_sql(SQL_QUERY_ITEM_MASTER, conn)

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