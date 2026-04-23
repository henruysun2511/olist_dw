import pandas as pd
import logging
from sqlalchemy import inspect

try:
    from dags.etl.config import get_pg_engine, get_sqlserver_conn, SOURCE_TABLES
except ImportError:
    from etl.config import get_pg_engine, get_sqlserver_conn, SOURCE_TABLES

def extract_table(src_table, stg_table, chunksize=50_000):
    conn = None
    try:
        conn = get_sqlserver_conn()
        engine = get_pg_engine('staging')
        first_chunk, total = True, 0
        
        # Đọc dữ liệu theo từng block
        for chunk in pd.read_sql(f'SELECT * FROM {src_table}', conn, chunksize=chunksize):
            
            # Xử lý đặc biệt cho các cột Zip Code để không mất số 0 ở đầu
            zip_cols = [c for c in chunk.columns if 'zip_code' in c]
            for col in zip_cols:
                chunk[col] = chunk[col].astype(str)

            mode = 'replace' if first_chunk else 'append'
            
            # Đẩy vào Postgres
            chunk.to_sql(stg_table, engine, if_exists=mode, index=False, method='multi')
            
            total += len(chunk)
            first_chunk = False
            logging.info(f'  ... nạp được {total:,} dòng vào {stg_table}')
            
        logging.info(f'[EXTRACT] Hoàn tất {src_table} → {stg_table}: {total:,} rows')
        
    except Exception as e:
        logging.error(f'Lỗi khi trích xuất bảng {src_table}: {e}')
        raise 
    finally:
        if conn:
            conn.close()

def extract_all_sources(**kwargs):
    logging.info("Bắt đầu quá trình trích xuất dữ liệu...")
    for stg_table, src_table in SOURCE_TABLES.items():
        extract_table(src_table, stg_table)