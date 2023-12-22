import os
import luigi
import pandas as pd
import pyodbc
from dotenv import load_dotenv

load_dotenv()


class ItemMaster(luigi.Task):
    MSSQL_SERVER = os.getenv('MSSQL_SERVER')
    DATABASE = os.getenv('DATABASE')
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    MSSQL_DRIVER = os.getenv('MSSQL_DRIVER')
    x = luigi.IntParameter()
    y = luigi.IntParameter(default=45)

    def run(self):
        print(self.x + self.y)

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

        SQL_QUERY = """
        SELECT [timestamp], No_, [No_ 2], Description, [Search Description], [Description 2], [Base Unit of Measure], [Price Unit Conversion], [Type], [Inventory Posting Group], [Shelf No_], [Item Disc_ Group], [Allow Invoice Disc_], [Statistics Group], [Commission Group], [Unit Price], [Price_Profit Calculation], [Profit _], [Costing Method], [Unit Cost], [Standard Cost], [Last Direct Cost], [Indirect Cost _], [Cost is Adjusted], [Allow Online Adjustment], [Vendor No_], [Vendor Item No_], [Lead Time Calculation], [Reorder Point], [Maximum Inventory], [Reorder Quantity], [Alternative Item No_], [Unit List Price], [Duty Due _], [Duty Code], [Gross Weight], [Net Weight], [Units per Parcel], [Unit Volume], Durability, [Freight Type], [Tariff No_], [Duty Unit Conversion], [Country_Region Purchased Code], [Budget Quantity], [Budgeted Amount], [Budget Profit], Blocked, [Block Reason], [Last DateTime Modified], [Last Date Modified], [Last Time Modified], [Price Includes VAT], [VAT Bus_ Posting Gr_ (Price)], [Gen_ Prod_ Posting Group], Picture, [Country_Region of Origin Code], [Automatic Ext_ Texts], [No_ Series], [Tax Group Code], [VAT Prod_ Posting Group], Reserve, [Global Dimension 1 Code], [Global Dimension 2 Code], [Stockout Warning], [Prevent Negative Inventory], [Variant Mandatory if Exists], [Application Wksh_ User ID], [Coupled to CRM], [Assembly Policy], GTIN, [Default Deferral Template Code], [Low-Level Code], [Lot Size], [Serial Nos_], [Last Unit Cost Calc_ Date], [Rolled-up Material Cost], [Rolled-up Capacity Cost], [Scrap _], [Inventory Value Zero], [Discrete Order Quantity], [Minimum Order Quantity], [Maximum Order Quantity], [Safety Stock Quantity], [Order Multiple], [Safety Lead Time], [Flushing Method], [Replenishment System], [Rounding Precision], [Sales Unit of Measure], [Purch_ Unit of Measure], [Time Bucket], [Reordering Policy], [Include Inventory], [Manufacturing Policy], [Rescheduling Period], [Lot Accumulation Period], [Dampener Period], [Dampener Quantity], [Overflow Level], [Manufacturer Code], [Item Category Code], [Created From Nonstock Item], [Product Group Code], [Purchasing Code], [Service Item Group], [Item Tracking Code], [Lot Nos_], [Expiration Calculation], [Warehouse Class Code], [Special Equipment Code], [Put-away Template Code], [Put-away Unit of Measure Code], [Phys Invt Counting Period Code], [Last Counting Period Update], [Use Cross-Docking], [Next Counting Start Date], [Next Counting End Date], Id, [Unit of Measure Id], [Tax Group Id], [Sales Blocked], [Purchasing Blocked], [Item Category Id], [Inventory Posting Group Id], [Gen_ Prod_ Posting Group Id], [Over-Receipt Code], [Duty Class], [SAT Item Classification], [SAT Hazardous Material], [SAT Packaging Type], [Routing No_], [Production BOM No_], [Single-Level Material Cost], [Single-Level Capacity Cost], [Single-Level Subcontrd_ Cost], [Single-Level Cap_ Ovhd Cost], [Single-Level Mfg_ Ovhd Cost], [Overhead Rate], [Rolled-up Subcontracted Cost], [Rolled-up Mfg_ Ovhd Cost], [Rolled-up Cap_ Overhead Cost], [Order Tracking Policy], Critical, [Common Item No_], [$systemId], [$systemCreatedAt], [$systemCreatedBy], [$systemModifiedAt], [$systemModifiedBy]
        FROM [Demo Database BC (23-0)].dbo.[CRONUS USA, Inc_$Item$437dbf0e-84ff-417a-965d-ed2bb9650972];
        """

        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        df = pd.read_sql(SQL_QUERY, conn)
        print(df.head(5))
        df.to_csv("output/itemmaster_20231222.csv")
