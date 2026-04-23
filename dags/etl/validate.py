import pandas as pd
import logging

# Setup logging configuration if not already set elsewhere
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
        """Checks a query result against zero or non-zero expectations."""
        result = pd.read_sql(query, engine).iloc[0, 0]
        # Handle cases where result might be None
        result = result if result is not None else 0
        
        ok = (result == 0) if expected_zero else (result > 0)
        status = 'PASS' if ok else 'FAIL'
        
        logging.info(f'[{status}] {name}: {result}')
        if not ok:
            errors.append(f'{name}: {result}')

    def check_nullable(name, count_null_query, total_query, threshold_pct=1.0):
        """Validates if the percentage of NULLs is within an acceptable threshold."""
        null_count = pd.read_sql(count_null_query, dw).iloc[0, 0]
        total = pd.read_sql(total_query, dw).iloc[0, 0]
        
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

    # --- 1. Row Count Reconciliation ---
    stg_items = pd.read_sql('SELECT COUNT(*) FROM stg_items', stg).iloc[0, 0]
    dw_sale   = pd.read_sql('SELECT COUNT(*) FROM fact_sale',  dw).iloc[0, 0]
    
    if stg_items != dw_sale:
        errors.append(f'Fact_Sale count mismatch: stg={stg_items} vs dw={dw_sale}')
    else:
        logging.info(f'[PASS] Row count reconciliation: {dw_sale} rows')

    # --- 2. Integrity Checks (NULLs & Duplicates) ---
    check('No NULL seller_key in Fact_Sale',
          'SELECT COUNT(*) FROM fact_sale WHERE seller_key IS NULL')
    
    check('No NULL product_key in Fact_Sale',
          'SELECT COUNT(*) FROM fact_sale WHERE product_key IS NULL')

    check('No duplicate order_id in Fact_Delivery',
          'SELECT COUNT(*) FROM (SELECT order_id FROM fact_delivery GROUP BY order_id HAVING COUNT(*) > 1) t')

    # --- 3. Threshold Checks ---
    check_nullable(
        'geo_customer_key NULL rate',
        'SELECT COUNT(*) FROM fact_delivery WHERE geo_customer_key IS NULL',
        'SELECT COUNT(*) FROM fact_delivery'
    )

    # --- 4. Financial Reconciliation ---
    rev_stg = pd.read_sql('SELECT SUM(price + freight_value) FROM stg_items', stg).iloc[0, 0] or 0
    rev_dw  = pd.read_sql('SELECT SUM(revenue_per_item) FROM fact_sale', dw).iloc[0, 0] or 0
    
    if rev_stg > 0:
        diff_pct = abs(rev_stg - rev_dw) / rev_stg * 100
        if diff_pct > 0.01:
            errors.append(f'Revenue mismatch: {diff_pct:.4f}% (STG: {rev_stg} vs DW: {rev_dw})')
        else:
            logging.info(f'[PASS] Revenue reconciliation: diff={diff_pct:.6f}%')
    elif rev_dw > 0:
        errors.append(f'Revenue mismatch: STG is 0 but DW is {rev_dw}')

    # --- Final Result ---
    if errors:
        error_msg = "\n".join(errors)
        raise ValueError(f'Validation FAILED:\n{error_msg}')
    
    logging.info('All DW validations PASSED')

if __name__ == "__main__":
    validate_dw()