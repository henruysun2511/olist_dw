import pandas as pd
import logging
 
try:
    from etl.config import get_pg_engine
except ImportError:
    from config import get_pg_engine
 
 
def to_date_key(series):
    return pd.to_datetime(series).dt.strftime('%Y%m%d').astype('Int64')
 
 
# ---------------------------------------------------------------------------
# Fact_Sale
# ---------------------------------------------------------------------------
 
def load_fact_sale(**kwargs):
    """
    Granularity: 1 record = 1 order_item trong 1 đơn hàng đã thanh toán.
    Thay đổi so với thiết kế gốc:
      - Xóa payment_value, payment_installments, payment_type_key
        (sai grain – chuyển sang Fact_Payment).
      - Xóa total_items_in_order, revenue_per_item (chuyển sang Dim_Order / tính trực tiếp).
      - Thêm order_key → Dim_Order.
    """
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')
 
    items  = pd.read_sql('SELECT * FROM stg_items', stg)
    orders = pd.read_sql(
        'SELECT order_id, customer_id, order_approved_at, order_purchase_timestamp'
        ' FROM stg_orders',
        stg,
    )
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
    orders['order_approved_at']        = pd.to_datetime(orders['order_approved_at'])
 
    dim_sel   = pd.read_sql('SELECT seller_key, seller_id FROM dim_seller WHERE is_current', dw)
    dim_prod  = pd.read_sql('SELECT product_key, product_id FROM dim_product', dw)
    dim_cust  = pd.read_sql('SELECT customer_key, customer_id FROM dim_customer', dw)
    dim_order = pd.read_sql('SELECT order_key, order_id FROM dim_order', dw)
 
    fact = items.merge(orders,    on='order_id',   how='left')
    fact = fact.merge(dim_sel,    on='seller_id',   how='left')
    fact = fact.merge(dim_prod,   on='product_id',  how='left')
    fact = fact.merge(dim_cust,   on='customer_id', how='left')
    fact = fact.merge(dim_order,  on='order_id',    how='left')
 
    fact['sale_date_key'] = to_date_key(fact['order_approved_at'])
    fact['purchase_hour'] = fact['order_purchase_timestamp'].dt.hour
 
    fact_sale = fact[[
        'sale_date_key',
        'customer_key',
        'seller_key',
        'product_key',
        'order_key',
        'order_id',       # Degenerate FK (giữ để JOIN cross-fact tiện)
        'order_item_id',
        'price',
        'freight_value',
        'purchase_hour',
    ]].copy()
 
    fact_sale.insert(0, 'sale_sk', range(1, len(fact_sale) + 1))
    fact_sale.to_sql('fact_sale', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Fact_Sale: {len(fact_sale):,} rows')
 
 
# ---------------------------------------------------------------------------
# Fact_Payment  (bảng mới)
# ---------------------------------------------------------------------------
 
def load_fact_payment(**kwargs):
    """
    Granularity: 1 record = 1 dòng thanh toán (payment_sequential) của 1 order_id.
    Lý do tách riêng: 1 đơn có thể có nhiều payment_sequential (credit_card + voucher…).
    Gộp vào Fact_Sale sẽ gây nhân đôi SUM(payment_value) theo số order_items.
    """
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')
 
    payments = pd.read_sql('SELECT * FROM stg_payments', stg)
    orders   = pd.read_sql(
        'SELECT order_id, order_approved_at FROM stg_orders', stg
    )
    orders['order_approved_at'] = pd.to_datetime(orders['order_approved_at'])
 
    dim_pay   = pd.read_sql('SELECT payment_type_key, payment_type FROM dim_paymenttype', dw)
    dim_order = pd.read_sql('SELECT order_key, order_id FROM dim_order', dw)
 
    fact = payments.merge(orders,    on='order_id',     how='left')
    fact = fact.merge(dim_pay,       on='payment_type', how='left')
    fact = fact.merge(dim_order,     on='order_id',     how='left')
 
    fact['payment_date_key'] = to_date_key(fact['order_approved_at'])
 
    fact_payment = fact[[
        'payment_date_key',
        'payment_type_key',
        'order_key',
        'order_id',               # Degenerate FK
        'payment_sequential',
        'payment_installments',
        'payment_value',
    ]].copy()
 
    fact_payment.insert(0, 'payment_sk', range(1, len(fact_payment) + 1))
    fact_payment.to_sql('fact_payment', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Fact_Payment: {len(fact_payment):,} rows')
 
 
# ---------------------------------------------------------------------------
# Fact_Delivery
# ---------------------------------------------------------------------------
 
def load_fact_delivery(**kwargs):
    """
    Granularity: 1 record = 1 order_id đã giao thành công (order_status = 'delivered').
    Thay đổi so với thiết kế gốc:
      - Bỏ status_key (Fact_Delivery chỉ chứa đơn 'delivered' nên không cần).
      - Thêm customer_key → Dim_Customer.
      - Thêm order_key → Dim_Order.
      - Thêm estimated_delivery_days (số ngày giao dự kiến theo cam kết).
    """
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')
 
    orders = pd.read_sql('SELECT * FROM stg_orders', stg)
    orders = orders[orders['order_status'] == 'delivered'].copy()
 
    ts_cols = [
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date',
    ]
    for col in ts_cols:
        orders[col] = pd.to_datetime(orders[col])
 
    # Measures (Derived)
    orders['delivery_days']            = (
        orders.order_delivered_customer_date - orders.order_purchase_timestamp
    ).dt.days
    orders['processing_days']          = (
        orders.order_delivered_carrier_date - orders.order_approved_at
    ).dt.days
    orders['transit_days']             = (
        orders.order_delivered_customer_date - orders.order_delivered_carrier_date
    ).dt.days
    orders['estimated_delivery_days']  = (
        orders.order_estimated_delivery_date - orders.order_purchase_timestamp
    ).dt.days
    orders['is_late']                  = (
        orders.order_delivered_customer_date > orders.order_estimated_delivery_date
    )
    orders['delay_days']               = (
        orders.order_delivered_customer_date - orders.order_estimated_delivery_date
    ).dt.days.clip(lower=0)
 
    # Dimension keys
    dim_sel   = pd.read_sql('SELECT seller_key, seller_id FROM dim_seller WHERE is_current', dw)
    dim_cust  = pd.read_sql('SELECT customer_key, customer_id FROM dim_customer', dw)
    dim_order = pd.read_sql('SELECT order_key, order_id FROM dim_order', dw)
    dim_geo   = pd.read_sql('SELECT geo_key, zip_code_prefix FROM dim_geolocation', dw)
 
    # Seller per order (lấy seller đầu tiên nếu đơn có nhiều seller)
    items = pd.read_sql('SELECT DISTINCT order_id, seller_id FROM stg_items', stg)
    items = items.drop_duplicates('order_id')
 
    # Zip code khách hàng
    custs = pd.read_sql('''
        SELECT o.order_id, c.customer_zip_code_prefix AS zip, c.customer_id
        FROM stg_customers c
        JOIN stg_orders o ON c.customer_id = o.customer_id
    ''', stg)
 
    # Zip code seller
    sels = pd.read_sql('SELECT seller_id, seller_zip_code_prefix AS zip FROM stg_sellers', stg)
 
    orders = orders.merge(items.drop_duplicates('order_id'), on='order_id', how='left')
    orders = orders.merge(dim_sel,  on='seller_id',   how='left')
    orders = orders.merge(dim_cust, on='customer_id', how='left')
    orders = orders.merge(dim_order, on='order_id',   how='left')
 
    # geo_customer_key
    orders = orders.merge(
        custs[['order_id', 'zip']],
        on='order_id', how='left'
    )
    orders = orders.merge(
        dim_geo.rename(columns={'zip_code_prefix': 'zip', 'geo_key': 'geo_customer_key'}),
        on='zip', how='left'
    )
 
    # geo_seller_key
    orders = orders.merge(
        sels.merge(
            dim_geo.rename(columns={'zip_code_prefix': 'zip', 'geo_key': 'geo_seller_key'}),
            on='zip', how='left'
        )[['seller_id', 'geo_seller_key']],
        on='seller_id', how='left'
    )
 
    orders['delivered_date_key'] = to_date_key(orders.order_delivered_customer_date)
    orders['approved_date_key']  = to_date_key(orders.order_approved_at)
 
    # freight_value tổng đơn
    freight = pd.read_sql(
        'SELECT order_id, SUM(freight_value) AS freight_value FROM stg_items GROUP BY order_id', stg
    )
    orders = orders.merge(freight, on='order_id', how='left', suffixes=('_old', ''))
    if 'freight_value_old' in orders.columns:
        orders.drop(columns=['freight_value_old'], inplace=True)
 
    # avg_weight_g
    items_prod = pd.read_sql('''
        SELECT i.order_id, AVG(p.product_weight_g) AS avg_weight_g
        FROM stg_items i
        JOIN stg_products p ON i.product_id = p.product_id
        GROUP BY i.order_id
    ''', stg)
    orders = orders.merge(items_prod, on='order_id', how='left')
 
    fact_delivery = orders[[
        'delivered_date_key',
        'approved_date_key',
        'seller_key',
        'customer_key',
        'order_key',
        'geo_customer_key',
        'geo_seller_key',
        'order_id',            # Degenerate FK
        'delivery_days',
        'processing_days',
        'transit_days',
        'estimated_delivery_days',
        'is_late',
        'delay_days',
        'freight_value',
        'avg_weight_g',
    ]].copy()
 
    fact_delivery.insert(0, 'delivery_sk', range(1, len(fact_delivery) + 1))
    fact_delivery.to_sql('fact_delivery', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Fact_Delivery: {len(fact_delivery):,} rows')
 
 
# ---------------------------------------------------------------------------
# Fact_Reviews
# ---------------------------------------------------------------------------
 
def load_fact_reviews(**kwargs):
    """
    Granularity: 1 record = 1 review_id (mỗi đơn hàng tối đa 1 đánh giá).
    Thay đổi so với thiết kế gốc:
      - Bỏ seller_key và product_key trực tiếp (tránh gán nhầm khi đơn có nhiều seller).
      - Thêm customer_key → Dim_Customer.
      - Thêm order_key → Dim_Order (thay thế order_id thô).
      - Đổi tên answer_time_h → answer_time_hours.
    """
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')
 
    reviews = pd.read_sql('SELECT * FROM stg_reviews', stg)
    reviews = reviews.dropna(subset=['review_score'])
    reviews['review_score'] = reviews['review_score'].astype(int)
 
    # Measures (Derived)
    reviews['is_negative']        = reviews['review_score'] <= 2
    reviews['has_comment']        = reviews['review_comment_message'].notna()
    reviews['answer_time_hours']  = (
        pd.to_datetime(reviews.review_answer_timestamp)
        - pd.to_datetime(reviews.review_creation_date)
    ).dt.total_seconds() / 3600
    reviews['review_score_label'] = reviews.review_score.map(
        {1: 'Poor', 2: 'Poor', 3: 'Average', 4: 'Good', 5: 'Excellent'}
    )
 
    # Dimension keys
    orders    = pd.read_sql('SELECT order_id, customer_id FROM stg_orders', stg)
    dim_cust  = pd.read_sql('SELECT customer_key, customer_id FROM dim_customer', dw)
    dim_order = pd.read_sql('SELECT order_key, order_id FROM dim_order', dw)
 
    reviews = reviews.merge(orders,    on='order_id',    how='left')
    reviews = reviews.merge(dim_cust,  on='customer_id', how='left')
    reviews = reviews.merge(dim_order, on='order_id',    how='left')
 
    reviews['review_date_key']   = to_date_key(reviews.review_answer_timestamp)
    reviews['creation_date_key'] = to_date_key(reviews.review_creation_date)
 
    fact_reviews = reviews[[
        'review_date_key',
        'creation_date_key',
        'customer_key',
        'order_key',
        'order_id',           # Degenerate FK
        'review_id',
        'review_score',
        'review_score_label',
        'is_negative',
        'has_comment',
        'answer_time_hours',
    ]].copy()
 
    fact_reviews.insert(0, 'review_sk', range(1, len(fact_reviews) + 1))
    fact_reviews.to_sql('fact_reviews', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Fact_Reviews: {len(fact_reviews):,} rows')
 