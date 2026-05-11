import pandas as pd
import logging

try:
    from etl.config import get_pg_engine
except ImportError:
    from config import get_pg_engine


def load_dim_date(**kwargs):
    dw = get_pg_engine('dw')

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
        '01-01', '04-21', '05-01', '09-07',
        '10-12', '11-02', '11-15', '12-25'
    }
    dim_date['is_holiday_brazil'] = dim_date.full_date.dt.strftime('%m-%d').isin(brazil_holidays)

    dim_date.to_sql('dim_date', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_Date: {len(dim_date)} records (2016–2018)')


def load_dim_customer(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')

    df = pd.read_sql('''
        SELECT customer_id, customer_unique_id,
               customer_zip_code_prefix AS zip_code_prefix,
               customer_city            AS city,
               customer_state           AS state
        FROM stg_customers
    ''', stg)
    df.drop_duplicates('customer_id', inplace=True)
    # Chuẩn hóa city về chữ thường
    df['city'] = df['city'].str.lower()
    df.insert(0, 'customer_key', range(1, len(df) + 1))
    df.to_sql('dim_customer', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_Customer: {len(df):,} rows')


def load_dim_seller(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')

    df = pd.read_sql('''
        SELECT seller_id,
               seller_zip_code_prefix AS zip_code_prefix,
               seller_city            AS city,
               seller_state           AS state
        FROM stg_sellers
    ''', stg)
    df.drop_duplicates('seller_id', inplace=True)

    # Chuẩn hóa city
    df['city'] = df['city'].str.lower()

    # Derived: region theo bang Brazil (5 vùng)
    _region_map = {
        'AC': 'North',       'AM': 'North',       'AP': 'North',
        'PA': 'North',       'RO': 'North',       'RR': 'North',       'TO': 'North',
        'AL': 'Northeast',   'BA': 'Northeast',   'CE': 'Northeast',   'MA': 'Northeast',
        'PB': 'Northeast',   'PE': 'Northeast',   'PI': 'Northeast',   'RN': 'Northeast',   'SE': 'Northeast',
        'DF': 'Center-West', 'GO': 'Center-West', 'MS': 'Center-West', 'MT': 'Center-West',
        'ES': 'Southeast',   'MG': 'Southeast',   'RJ': 'Southeast',   'SP': 'Southeast',
        'PR': 'South',       'RS': 'South',       'SC': 'South',
    }
    df['region'] = df['state'].str.upper().map(_region_map).fillna('Unknown')

    # first_order_date: ngày đơn hàng đầu tiên của mỗi seller (từ stg_orders + stg_items)
    first_orders = pd.read_sql('''
        SELECT i.seller_id,
               MIN(o.order_purchase_timestamp)::date AS first_order_date
        FROM stg_items i
        JOIN stg_orders o ON i.order_id = o.order_id
        GROUP BY i.seller_id
    ''', stg)
    df = df.merge(first_orders, on='seller_id', how='left')

    df.insert(0, 'seller_key', range(1, len(df) + 1))
    df['start_date'] = pd.Timestamp('2016-01-01').date()
    df['end_date']   = None
    df['is_current'] = True

    df.to_sql('dim_seller', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_Seller: {len(df):,} rows')


def load_dim_product(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')

    df = pd.read_sql('''
        SELECT product_id,
               product_category_name,
               product_name_lenght         AS name_length,
               product_description_lenght  AS description_length,
               product_photos_qty          AS photos_qty,
               product_weight_g            AS weight_g,
               product_length_cm           AS length_cm,
               product_height_cm           AS height_cm,
               product_width_cm            AS width_cm
        FROM stg_products
    ''', stg)
    df.drop_duplicates('product_id', inplace=True)

    # Thử join bảng dịch — nếu không tồn tại thì dùng tên tiếng Bồ Đào Nha làm fallback
    try:
        # Kiểm tra tên bảng thực tế trong staging
        from sqlalchemy import inspect
        inspector = inspect(stg)
        stg_tables = inspector.get_table_names(schema='staging')
        trans_table = next(
            (t for t in stg_tables if 'translation' in t.lower() or 'category_name' in t.lower()),
            None
        )
        if trans_table:
            trans = pd.read_sql(
                f'SELECT product_category_name, product_category_name_english'
                f' FROM staging.{trans_table}',
                stg
            )
            df = df.merge(trans, on='product_category_name', how='left')
            df['category_name_en'] = df['product_category_name_english'].fillna(
                df['product_category_name']
            )
            df.drop(columns=['product_category_name_english'], inplace=True)
        else:
            logging.warning('Không tìm thấy bảng dịch category — dùng tên gốc tiếng Bồ Đào Nha')
            df['category_name_en'] = df['product_category_name']
    except Exception as e:
        logging.warning(f'Bỏ qua bảng dịch category: {e}')
        df['category_name_en'] = df['product_category_name']

    # Giữ tên tiếng Bồ Đào Nha để tham chiếu nguồn
    df.rename(columns={'product_category_name': 'category_name_pt'}, inplace=True)

    # Derived: volume_cm3 = length × height × width
    df['product_volume_cm3'] = df['length_cm'] * df['height_cm'] * df['width_cm']

    # Derived: size_bucket theo thể tích (cm3)
    def _size_bucket(v):
        if pd.isna(v):
            return 'Unknown'
        if v < 1_000:
            return 'Small'
        if v < 10_000:
            return 'Medium'
        if v < 50_000:
            return 'Large'
        return 'XLarge'

    df['size_bucket'] = df['product_volume_cm3'].apply(_size_bucket)

    df.insert(0, 'product_key', range(1, len(df) + 1))
    df.to_sql('dim_product', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_Product: {len(df):,} rows')


def load_dim_geolocation(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')

    df = pd.read_sql('''
        SELECT geolocation_zip_code_prefix AS zip_code_prefix,
               AVG(geolocation_lat)        AS avg_lat,
               AVG(geolocation_lng)        AS avg_lng,
               MODE() WITHIN GROUP (ORDER BY geolocation_city)  AS city,
               MAX(geolocation_state)      AS state
        FROM stg_geo
        GROUP BY geolocation_zip_code_prefix
    ''', stg)

    # Derived: region
    _region_map = {
        'AC': 'North',       'AM': 'North',       'AP': 'North',
        'PA': 'North',       'RO': 'North',       'RR': 'North',       'TO': 'North',
        'AL': 'Northeast',   'BA': 'Northeast',   'CE': 'Northeast',   'MA': 'Northeast',
        'PB': 'Northeast',   'PE': 'Northeast',   'PI': 'Northeast',   'RN': 'Northeast',   'SE': 'Northeast',
        'DF': 'Center-West', 'GO': 'Center-West', 'MS': 'Center-West', 'MT': 'Center-West',
        'ES': 'Southeast',   'MG': 'Southeast',   'RJ': 'Southeast',   'SP': 'Southeast',
        'PR': 'South',       'RS': 'South',       'SC': 'South',
    }
    df['region'] = df['state'].str.upper().map(_region_map).fillna('Unknown')
    df['city']   = df['city'].str.lower()

    df.insert(0, 'geo_key', range(1, len(df) + 1))
    df.to_sql('dim_geolocation', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_Geolocation: {len(df):,} rows')


def load_dim_paymenttype(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')

    df = pd.read_sql('SELECT DISTINCT payment_type FROM stg_payments', stg)
    df.dropna(inplace=True)

    # Mô tả tiếng Việt
    _desc_vn = {
        'credit_card': 'Thẻ tín dụng – hỗ trợ trả góp lên đến 24 kỳ',
        'boleto':      'Boleto bancário – phiếu thanh toán qua ngân hàng',
        'voucher':     'Phiếu giảm giá / mã khuyến mãi',
        'debit_card':  'Thẻ ghi nợ – thanh toán 1 lần',
        'not_defined': 'Phương thức chưa xác định',
    }
    df['description_vn'] = df['payment_type'].map(_desc_vn).fillna('')

    df.insert(0, 'payment_type_key', range(1, len(df) + 1))
    df.to_sql('dim_paymenttype', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_PaymentType: {len(df):,} rows')


def load_dim_orderstatus(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')

    df = pd.read_sql('SELECT DISTINCT order_status FROM stg_orders', stg)
    df.dropna(inplace=True)

    # Các trạng thái kết thúc vòng đời đơn hàng
    _terminal  = {'delivered', 'unavailable', 'canceled'}
    _success   = {'delivered'}
    _desc_vn = {
        'created':     'Đơn vừa tạo, chờ thanh toán',
        'approved':    'Thanh toán đã xác nhận',
        'invoiced':    'Hóa đơn điện tử đã phát hành (yêu cầu pháp lý Brazil)',
        'processing':  'Seller đang chuẩn bị hàng',
        'shipped':     'Đã giao cho đơn vị vận chuyển',
        'delivered':   'Khách đã nhận hàng thành công',
        'unavailable': 'Hết hàng – đơn bị hủy tự động',
        'canceled':    'Đơn bị hủy bởi khách hàng hoặc seller',
    }

    df['is_terminal']    = df['order_status'].isin(_terminal)
    df['is_success']     = df['order_status'].isin(_success)
    df['description_vn'] = df['order_status'].map(_desc_vn).fillna('')

    df.insert(0, 'status_key', range(1, len(df) + 1))
    df.to_sql('dim_orderstatus', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_OrderStatus: {len(df):,} rows')


def load_dim_order(**kwargs):
    """Bảng Dimension mới – mỗi record là 1 đơn hàng duy nhất."""
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')

    orders = pd.read_sql('''
        SELECT order_id,
               customer_id,
               order_status,
               order_purchase_timestamp,
               order_approved_at,
               order_estimated_delivery_date
        FROM stg_orders
    ''', stg)

    # Join khóa ngoại từ các dim đã có
    dim_cust   = pd.read_sql('SELECT customer_key, customer_id FROM dim_customer', dw)
    dim_status = pd.read_sql('SELECT status_key, order_status FROM dim_orderstatus', dw)

    orders = orders.merge(dim_cust,   on='customer_id',   how='left')
    orders = orders.merge(dim_status, on='order_status',  how='left')

    def to_date_key(series):
        return pd.to_datetime(series).dt.strftime('%Y%m%d').astype('Int64')

    orders['purchase_date_key']       = to_date_key(orders['order_purchase_timestamp'])
    orders['approved_date_key']       = to_date_key(orders['order_approved_at'])
    orders['estimated_delivery_key']  = to_date_key(orders['order_estimated_delivery_date'])

    # item_count: tổng order_items mỗi đơn
    item_counts = pd.read_sql(
        'SELECT order_id, COUNT(*) AS item_count FROM stg_items GROUP BY order_id', stg
    )
    orders = orders.merge(item_counts, on='order_id', how='left')

    dim_order = orders[[
        'order_id',
        'customer_key',
        'status_key',           # order_status_key
        'purchase_date_key',
        'approved_date_key',
        'estimated_delivery_key',
        'item_count',
    ]].copy()
    dim_order.rename(columns={'status_key': 'order_status_key'}, inplace=True)
    dim_order.insert(0, 'order_key', range(1, len(dim_order) + 1))

    dim_order.to_sql('dim_order', dw, if_exists='replace', index=False, schema='dw')
    logging.info(f'Dim_Order: {len(dim_order):,} rows')