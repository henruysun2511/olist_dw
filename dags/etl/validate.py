import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

try:
    from etl.config import get_pg_engine
except ImportError:
    from config import get_pg_engine


def validate_dw(**kwargs):
    stg = get_pg_engine('staging')
    dw  = get_pg_engine('dw')
    errors = []

    # --- Helper Functions ---
    def check(name, query, engine=dw, expected_zero=True):
        result = pd.read_sql(query, engine).iloc[0, 0]
        result = result if result is not None else 0
        ok = (result == 0) if expected_zero else (result > 0)
        status = 'PASS' if ok else 'FAIL'
        logging.info(f'[{status}] {name}: {result}')
        if not ok:
            errors.append(f'{name}: {result}')

    def check_nullable(name, count_null_query, total_query, threshold_pct=5.0):
        """Cho phép tỉ lệ NULL tối đa threshold_pct% — log WARN, không FAIL."""
        null_count = pd.read_sql(count_null_query, dw).iloc[0, 0]
        total      = pd.read_sql(total_query,       dw).iloc[0, 0]
        if total == 0:
            logging.warning(f'[SKIP] {name}: Total count is 0.')
            return
        pct = (null_count / total) * 100
        if pct > threshold_pct:
            msg = f'{name}: {null_count} NULLs ({pct:.2f}%) > {threshold_pct}%'
            errors.append(msg)
            logging.warning(f'[WARN] {msg}')
        else:
            logging.info(f'[PASS] {name}: {null_count} NULLs ({pct:.2f}%) — within threshold')

    def warn_nullable(name, count_null_query, total_query):
        """Chỉ log WARNING, không đưa vào errors — dùng cho cột nullable theo nghiệp vụ."""
        null_count = pd.read_sql(count_null_query, dw).iloc[0, 0]
        total      = pd.read_sql(total_query,       dw).iloc[0, 0]
        pct = (null_count / total * 100) if total > 0 else 0
        logging.warning(f'[INFO] {name}: {null_count} NULLs ({pct:.2f}%) — expected nullable')

    # =========================================================
    # 1. ROW COUNT RECONCILIATION
    # =========================================================

    stg_items = pd.read_sql('SELECT COUNT(*) FROM stg_items', stg).iloc[0, 0]
    dw_sale   = pd.read_sql('SELECT COUNT(*) FROM dw.fact_sale', dw).iloc[0, 0]
    if stg_items != dw_sale:
        errors.append(f'Fact_Sale count mismatch: stg={stg_items} vs dw={dw_sale}')
    else:
        logging.info(f'[PASS] Fact_Sale row count: {dw_sale}')

    stg_pay = pd.read_sql('SELECT COUNT(*) FROM stg_payments', stg).iloc[0, 0]
    dw_pay  = pd.read_sql('SELECT COUNT(*) FROM dw.fact_payment', dw).iloc[0, 0]
    if stg_pay != dw_pay:
        errors.append(f'Fact_Payment count mismatch: stg={stg_pay} vs dw={dw_pay}')
    else:
        logging.info(f'[PASS] Fact_Payment row count: {dw_pay}')

    stg_delivered = pd.read_sql(
        "SELECT COUNT(*) FROM stg_orders WHERE order_status = 'delivered'", stg
    ).iloc[0, 0]
    dw_delivery = pd.read_sql('SELECT COUNT(*) FROM dw.fact_delivery', dw).iloc[0, 0]
    if stg_delivered != dw_delivery:
        errors.append(f'Fact_Delivery count mismatch: stg={stg_delivered} vs dw={dw_delivery}')
    else:
        logging.info(f'[PASS] Fact_Delivery row count: {dw_delivery}')

    stg_orders = pd.read_sql('SELECT COUNT(*) FROM stg_orders', stg).iloc[0, 0]
    dw_orders  = pd.read_sql('SELECT COUNT(*) FROM dw.dim_order', dw).iloc[0, 0]
    if stg_orders != dw_orders:
        errors.append(f'Dim_Order count mismatch: stg={stg_orders} vs dw={dw_orders}')
    else:
        logging.info(f'[PASS] Dim_Order row count: {dw_orders}')

    # =========================================================
    # 2. FOREIGN KEY INTEGRITY
    # =========================================================

    # Fact_Sale — các FK bắt buộc
    check('No NULL seller_key in Fact_Sale',
          'SELECT COUNT(*) FROM dw.fact_sale WHERE seller_key IS NULL')
    check('No NULL product_key in Fact_Sale',
          'SELECT COUNT(*) FROM dw.fact_sale WHERE product_key IS NULL')
    check('No NULL customer_key in Fact_Sale',
          'SELECT COUNT(*) FROM dw.fact_sale WHERE customer_key IS NULL')
    check('No NULL order_key in Fact_Sale',
          'SELECT COUNT(*) FROM dw.fact_sale WHERE order_key IS NULL')

    # sale_date_key: nullable vì một số đơn chưa được approve (order_approved_at = NULL)
    warn_nullable(
        'sale_date_key nullable (unapproved orders)',
        'SELECT COUNT(*) FROM dw.fact_sale WHERE sale_date_key IS NULL',
        'SELECT COUNT(*) FROM dw.fact_sale'
    )

    # Fact_Payment
    check('No NULL payment_type_key in Fact_Payment',
          'SELECT COUNT(*) FROM dw.fact_payment WHERE payment_type_key IS NULL')
    check('No NULL order_key in Fact_Payment',
          'SELECT COUNT(*) FROM dw.fact_payment WHERE order_key IS NULL')

    # Fact_Delivery
    check('No NULL seller_key in Fact_Delivery',
          'SELECT COUNT(*) FROM dw.fact_delivery WHERE seller_key IS NULL')
    check('No NULL customer_key in Fact_Delivery',
          'SELECT COUNT(*) FROM dw.fact_delivery WHERE customer_key IS NULL')
    check('No NULL order_key in Fact_Delivery',
          'SELECT COUNT(*) FROM dw.fact_delivery WHERE order_key IS NULL')
    check('No duplicate order_id in Fact_Delivery',
          'SELECT COUNT(*) FROM ('
          '  SELECT order_id FROM dw.fact_delivery'
          '  GROUP BY order_id HAVING COUNT(*) > 1'
          ') t')

    # Fact_Reviews — customer_key và order_key nullable vì review có thể không match order
    warn_nullable(
        'customer_key nullable in Fact_Reviews (unmatched reviews)',
        'SELECT COUNT(*) FROM dw.fact_reviews WHERE customer_key IS NULL',
        'SELECT COUNT(*) FROM dw.fact_reviews'
    )
    warn_nullable(
        'order_key nullable in Fact_Reviews (unmatched reviews)',
        'SELECT COUNT(*) FROM dw.fact_reviews WHERE order_key IS NULL',
        'SELECT COUNT(*) FROM dw.fact_reviews'
    )

    # =========================================================
    # 3. NULLABLE THRESHOLD CHECKS
    # =========================================================
    check_nullable(
        'geo_customer_key NULL rate in Fact_Delivery',
        'SELECT COUNT(*) FROM dw.fact_delivery WHERE geo_customer_key IS NULL',
        'SELECT COUNT(*) FROM dw.fact_delivery',
        threshold_pct=5.0
    )
    check_nullable(
        'geo_seller_key NULL rate in Fact_Delivery',
        'SELECT COUNT(*) FROM dw.fact_delivery WHERE geo_seller_key IS NULL',
        'SELECT COUNT(*) FROM dw.fact_delivery',
        threshold_pct=5.0
    )
    # category_name_en: ~1.85% sản phẩm gốc không có category — dùng threshold 5%
    check_nullable(
        'category_name_en NULL rate in Dim_Product',
        'SELECT COUNT(*) FROM dw.dim_product WHERE category_name_en IS NULL',
        'SELECT COUNT(*) FROM dw.dim_product',
        threshold_pct=5.0
    )

    # =========================================================
    # 4. FINANCIAL RECONCILIATION
    # =========================================================

    rev_stg = pd.read_sql(
        'SELECT SUM(price + freight_value) FROM stg_items', stg
    ).iloc[0, 0] or 0
    rev_dw = pd.read_sql(
        'SELECT SUM(price + freight_value) FROM dw.fact_sale', dw
    ).iloc[0, 0] or 0

    if rev_stg > 0:
        diff_pct = abs(rev_stg - rev_dw) / rev_stg * 100
        if diff_pct > 0.01:
            errors.append(
                f'Revenue mismatch: {diff_pct:.4f}% (STG: {rev_stg:.2f} vs DW: {rev_dw:.2f})'
            )
        else:
            logging.info(f'[PASS] Revenue reconciliation: diff={diff_pct:.6f}%')
    elif rev_dw > 0:
        errors.append(f'Revenue mismatch: STG=0 but DW={rev_dw}')

    pay_stg = pd.read_sql(
        'SELECT SUM(payment_value) FROM stg_payments', stg
    ).iloc[0, 0] or 0
    pay_dw = pd.read_sql(
        'SELECT SUM(payment_value) FROM dw.fact_payment', dw
    ).iloc[0, 0] or 0

    if pay_stg > 0:
        diff_pct = abs(pay_stg - pay_dw) / pay_stg * 100
        if diff_pct > 0.01:
            errors.append(
                f'Payment value mismatch: {diff_pct:.4f}%'
                f' (STG: {pay_stg:.2f} vs DW: {pay_dw:.2f})'
            )
        else:
            logging.info(f'[PASS] Payment value reconciliation: diff={diff_pct:.6f}%')

    # =========================================================
    # 5. BUSINESS LOGIC CHECKS
    # =========================================================

    # delay_days phải >= 0 (clip(lower=0) đã xử lý trong ETL)
    check('No negative delay_days in Fact_Delivery',
          'SELECT COUNT(*) FROM dw.fact_delivery WHERE delay_days < 0')

    # delivery_days = 0 có thể xảy ra (giao cùng ngày) — chỉ warn
    warn_nullable(
        'delivery_days = 0 (same-day delivery — expected)',
        'SELECT COUNT(*) FROM dw.fact_delivery WHERE delivery_days = 0',
        'SELECT COUNT(*) FROM dw.fact_delivery'
    )

    # delivery_days âm là lỗi thực sự
    check('No negative delivery_days in Fact_Delivery',
          'SELECT COUNT(*) FROM dw.fact_delivery WHERE delivery_days < 0')

    # review_score phải trong khoảng 1-5
    check('review_score in valid range (1-5)',
          'SELECT COUNT(*) FROM dw.fact_reviews WHERE review_score NOT BETWEEN 1 AND 5')

    # =========================================================
    # FINAL RESULT
    # =========================================================
    if errors:
        error_msg = '\n'.join(errors)
        raise ValueError(f'Validation FAILED:\n{error_msg}')

    logging.info('All DW validations PASSED')


if __name__ == '__main__':
    validate_dw()