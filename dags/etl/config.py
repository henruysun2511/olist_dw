import os
from sqlalchemy import create_engine
import pyodbc
from dotenv import load_dotenv

load_dotenv()

# --- SQL Server ---
SQLSERVER_CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('MSSQL_HOST')};"
    f"DATABASE={os.getenv('MSSQL_DB')};"
    f"UID={os.getenv('MSSQL_USER')};"
    f"PWD={os.getenv('MSSQL_PASS')};"
)

def get_sqlserver_conn():
    return pyodbc.connect(SQLSERVER_CONN_STR)

# --- PostgreSQL ---
PG_USER = os.getenv('PG_USER')
PG_PASS = os.getenv('PG_PASS')
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB   = os.getenv('PG_DB')

def get_pg_engine(db_type='staging'):
    # ✅ FIX: dùng db_type làm schema — staging và dw là 2 schema khác nhau trong cùng 1 database
    url = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    engine = create_engine(url)
    # Set search_path theo schema
    with engine.connect() as conn:
        conn.execute(__import__('sqlalchemy').text(f"SET search_path TO {db_type}"))
        conn.commit()
    return engine

# --- SOURCE_TABLES ---
SOURCE_TABLES = {
    'stg_orders':     'dbo.olist_orders',
    'stg_items':      'dbo.olist_order_items',
    'stg_customers':  'dbo.olist_customers',
    'stg_sellers':    'dbo.olist_sellers',
    'stg_products':   'dbo.olist_products',
    'stg_payments':   'dbo.olist_order_payments',
    'stg_reviews':    'dbo.olist_order_reviews',
    'stg_geo':        'dbo.olist_geolocation',
    'stg_categories': 'dbo.product_category_translation',  # ✅ FIX: thụt lề 4 spaces
}