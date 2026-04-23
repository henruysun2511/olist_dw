import pandas as pd
import logging

# ✅ FIX: bỏ dòng import ngoài try/except
try:
    from etl.config import get_pg_engine
except ImportError:
    from config import get_pg_engine

def to_date_key(series):
    return pd.to_datetime(series).dt.strftime('%Y%m%d').astype('Int64')

def load_fact_sale(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')

    items    = pd.read_sql('SELECT * FROM stg_items', stg)
    payments = pd.read_sql(
        'SELECT order_id, payment_type, payment_installments, payment_value'
        ' FROM stg_payments WHERE payment_sequential = 1', stg
    )
    orders = pd.read_sql(
        'SELECT order_id, customer_id, order_approved_at, order_purchase_timestamp'
        ' FROM stg_orders', stg
    )
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
    orders['order_approved_at']        = pd.to_datetime(orders['order_approved_at'])

    dim_sel  = pd.read_sql('SELECT seller_key, seller_id FROM dim_seller WHERE is_current', dw)
    dim_prod = pd.read_sql('SELECT product_key, product_id FROM dim_product', dw)
    dim_cust = pd.read_sql('SELECT customer_key, customer_id FROM dim_customer', dw)
    dim_pay  = pd.read_sql('SELECT payment_type_key, payment_type FROM dim_paymenttype', dw)

    fact = items.merge(orders,    on='order_id', how='left')
    fact = fact.merge(payments,   on='order_id', how='left')
    fact = fact.merge(dim_sel,    on='seller_id',    how='left')
    fact = fact.merge(dim_prod,   on='product_id',   how='left')
    fact = fact.merge(dim_cust,   on='customer_id',  how='left')
    fact = fact.merge(dim_pay,    on='payment_type', how='left')

    fact['sale_date_key']        = to_date_key(fact['order_approved_at'])
    fact['revenue_per_item']     = fact['price'] + fact['freight_value']
    fact['purchase_hour']        = fact['order_purchase_timestamp'].dt.hour
    fact['total_items_in_order'] = fact.groupby('order_id')['order_item_id'].transform('count')

    fact_sale = fact[[
        'sale_date_key','customer_key','seller_key','product_key','payment_type_key',
        'order_id','order_item_id','price','freight_value','revenue_per_item',
        'payment_value','payment_installments','total_items_in_order','purchase_hour'
    ]].copy()
    fact_sale.insert(0, 'sale_sk', range(1, len(fact_sale)+1))
    fact_sale.to_sql('fact_sale', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Fact_Sale: {len(fact_sale):,} rows')

def load_fact_delivery(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')

    orders = pd.read_sql('SELECT * FROM stg_orders', stg)
    orders = orders[orders['order_status'] == 'delivered'].copy()

    ts_cols = ['order_purchase_timestamp','order_approved_at',
               'order_delivered_carrier_date',
               'order_delivered_customer_date',
               'order_estimated_delivery_date']
    for col in ts_cols:
        orders[col] = pd.to_datetime(orders[col])

    orders['delivery_days']   = (orders.order_delivered_customer_date - orders.order_purchase_timestamp).dt.days
    orders['processing_days'] = (orders.order_delivered_carrier_date  - orders.order_approved_at).dt.days
    orders['transit_days']    = (orders.order_delivered_customer_date  - orders.order_delivered_carrier_date).dt.days
    orders['is_late']         = orders.order_delivered_customer_date > orders.order_estimated_delivery_date
    orders['delay_days']      = (orders.order_delivered_customer_date - orders.order_estimated_delivery_date).dt.days.clip(lower=0)

    dim_sel = pd.read_sql('SELECT seller_key, seller_id FROM dim_seller WHERE is_current', dw)
    dim_geo = pd.read_sql('SELECT geo_key, zip_code_prefix FROM dim_geolocation', dw)
    dim_sts = pd.read_sql('SELECT status_key, order_status FROM dim_orderstatus', dw)

    items = pd.read_sql('SELECT DISTINCT order_id, seller_id FROM stg_items', stg)

    custs = pd.read_sql('''
        SELECT o.order_id, c.customer_zip_code_prefix AS zip
        FROM stg_customers c
        JOIN stg_orders o ON c.customer_id = o.customer_id
    ''', stg)

    sels = pd.read_sql('SELECT seller_id, seller_zip_code_prefix AS zip FROM stg_sellers', stg)

    orders = orders.merge(items.drop_duplicates('order_id'), on='order_id', how='left')
    orders = orders.merge(dim_sel, on='seller_id', how='left')
    orders = orders.merge(custs,   on='order_id',  how='left')
    orders = orders.merge(
        dim_geo.rename(columns={'zip_code_prefix':'zip','geo_key':'geo_customer_key'}),
        on='zip', how='left'
    )
    orders = orders.merge(
        sels.merge(
            dim_geo.rename(columns={'zip_code_prefix':'zip','geo_key':'geo_seller_key'}),
            on='zip', how='left'
        )[['seller_id','geo_seller_key']],
        on='seller_id', how='left'
    )
    orders = orders.merge(dim_sts, on='order_status', how='left')

    orders['delivered_date_key'] = to_date_key(orders.order_delivered_customer_date)
    orders['approved_date_key']  = to_date_key(orders.order_approved_at)

    freight = pd.read_sql(
        'SELECT order_id, SUM(freight_value) AS freight_value FROM stg_items GROUP BY order_id', stg
    )
    orders = orders.merge(freight, on='order_id', how='left', suffixes=('_old',''))
    if 'freight_value_old' in orders.columns:
        orders.drop(columns=['freight_value_old'], inplace=True)

    items_prod = pd.read_sql('''
        SELECT i.order_id, AVG(p.product_weight_g) AS avg_weight_g
        FROM stg_items i
        JOIN stg_products p ON i.product_id = p.product_id
        GROUP BY i.order_id
    ''', stg)
    orders = orders.merge(items_prod, on='order_id', how='left')

    fact_delivery = orders[[
        'delivered_date_key','approved_date_key','seller_key', 'status_key'
        'geo_customer_key','geo_seller_key',
        'order_id','delivery_days','processing_days','transit_days',
        'is_late','delay_days','freight_value','avg_weight_g'
    ]].copy()
    fact_delivery.insert(0, 'delivery_sk', range(1, len(fact_delivery)+1))
    fact_delivery.to_sql('fact_delivery', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Fact_Delivery: {len(fact_delivery):,} rows')

def load_fact_reviews(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')

    reviews = pd.read_sql('SELECT * FROM stg_reviews', stg)
    reviews = reviews.dropna(subset=['review_score'])
    reviews['review_score']       = reviews['review_score'].astype(int)
    reviews['is_negative']        = reviews['review_score'] <= 2
    reviews['has_comment']        = reviews['review_comment_message'].notna()
    reviews['answer_time_h']      = (
        pd.to_datetime(reviews.review_answer_timestamp)
        - pd.to_datetime(reviews.review_creation_date)
    ).dt.total_seconds() / 3600
    reviews['review_score_label'] = reviews.review_score.map(
        {1:'Poor',2:'Poor',3:'Average',4:'Good',5:'Excellent'}
    )

    sale_keys = pd.read_sql(
        'SELECT order_id, seller_key, product_key, customer_key'
        ' FROM fact_sale WHERE order_item_id = 1', dw
    )
    reviews = reviews.merge(sale_keys, on='order_id', how='left')
    reviews['review_date_key']   = to_date_key(reviews.review_answer_timestamp)
    reviews['creation_date_key'] = to_date_key(reviews.review_creation_date)

    fact_reviews = reviews[[
        'review_date_key','creation_date_key','seller_key','product_key','customer_key',
        'order_id','review_id','review_score','review_score_label',
        'is_negative','has_comment','answer_time_h'
    ]].copy()
    fact_reviews.insert(0, 'review_sk', range(1, len(fact_reviews)+1))
    fact_reviews.to_sql('fact_reviews', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Fact_Reviews: {len(fact_reviews):,} rows')