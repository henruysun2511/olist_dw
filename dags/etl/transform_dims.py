import pandas as pd
import logging

# ✅ FIX: bỏ dòng import ngoài try/except, chỉ giữ try/except
try:
    from etl.config import get_pg_engine
except ImportError:
    from config import get_pg_engine

def load_dim_date(**kwargs):
    dw = get_pg_engine('dw')
    import numpy as np

    dates = pd.date_range('2016-01-01', '2018-12-31', freq='D')
    dim_date = pd.DataFrame({'full_date': dates})

    dim_date['date_key']          = dim_date.full_date.dt.strftime('%Y%m%d').astype(int)
    dim_date['day_of_month']      = dim_date.full_date.dt.day
    dim_date['day_of_week']       = dim_date.full_date.dt.dayofweek + 1  # 1=Mon, 7=Sun
    dim_date['month']             = dim_date.full_date.dt.month
    dim_date['quarter']           = dim_date.full_date.dt.quarter
    dim_date['year']              = dim_date.full_date.dt.year
    dim_date['is_weekend']        = dim_date.full_date.dt.dayofweek >= 5

    brazil_holidays = {
        '01-01','04-21','05-01','09-07',
        '10-12','11-02','11-15','12-25'
    }
    dim_date['is_holiday_brazil'] = dim_date.full_date.dt.strftime('%m-%d').isin(brazil_holidays)

    dim_date.to_sql('dim_date', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_Date: {len(dim_date)} records (2016–2018)')

def load_dim_customer(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')
    df = pd.read_sql('''
        SELECT customer_id, customer_unique_id,
               customer_zip_code_prefix, customer_city, customer_state
        FROM stg_customers
    ''', stg)
    df.drop_duplicates('customer_id', inplace=True)
    df.insert(0, 'customer_key', range(1, len(df)+1))
    df.to_sql('dim_customer', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_Customer: {len(df):,} rows')

def load_dim_seller(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')
    df = pd.read_sql('''
        SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state
        FROM stg_sellers
    ''', stg)
    df.drop_duplicates('seller_id', inplace=True)
    df.insert(0, 'seller_key', range(1, len(df)+1))
    df['is_current'] = True
    df.to_sql('dim_seller', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_Seller: {len(df):,} rows')

def load_dim_product(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')
    df = pd.read_sql('''
        SELECT product_id, product_category_name,
               product_name_lenght, product_description_lenght,
               product_weight_g
        FROM stg_products
    ''', stg)
    df.drop_duplicates('product_id', inplace=True)
    df.insert(0, 'product_key', range(1, len(df)+1))
    df.rename(columns={
        'product_name_lenght':        'product_name_length',
        'product_description_lenght': 'product_description_length'
    }, inplace=True)
    df.to_sql('dim_product', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_Product: {len(df):,} rows')

def load_dim_paymenttype(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')
    df = pd.read_sql('SELECT DISTINCT payment_type FROM stg_payments', stg)
    df.dropna(inplace=True)
    df.insert(0, 'payment_type_key', range(1, len(df)+1))
    df.to_sql('dim_paymenttype', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_PaymentType: {len(df):,} rows')

def load_dim_orderstatus(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')
    df = pd.read_sql('SELECT DISTINCT order_status FROM stg_orders', stg)
    df.dropna(inplace=True)
    df.insert(0, 'status_key', range(1, len(df)+1))
    df.to_sql('dim_orderstatus', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_OrderStatus: {len(df):,} rows')

def load_dim_geolocation(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')
    df = pd.read_sql('''
        SELECT geolocation_zip_code_prefix AS zip_code_prefix,
               geolocation_lat, geolocation_lng,
               geolocation_city, geolocation_state
        FROM stg_geo
    ''', stg)
    df.drop_duplicates('zip_code_prefix', inplace=True)
    df.insert(0, 'geo_key', range(1, len(df)+1))
    df.to_sql('dim_geolocation', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_Geolocation: {len(df):,} rows')