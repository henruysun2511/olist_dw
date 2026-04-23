from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.extract       import extract_all_sources
from etl.transform_dims import (transform_dim_date, transform_dim_geolocation,
                                transform_dim_static, transform_dim_customer,
                                transform_dim_seller, transform_dim_product)
from etl.transform_facts import (load_fact_sale, load_fact_delivery, load_fact_reviews)
from etl.validate       import validate_dw

default_args = {
    'owner':           'data_team',
    'depends_on_past':  False,
    'retries':          2,
    'retry_delay':      timedelta(minutes=10),
    'email_on_failure': True,
    'email':           ['data-team@olist.com'],
}

with DAG(
    dag_id          = 'olist_dw_etl',
    description     = 'ETL pipeline: SQL Server → PostgreSQL DW (Star Schema)',
    schedule_interval = '0 2 * * *',  # Chạy lúc 2:00 AM mỗi ngày
    start_date      = datetime(2024, 1, 1),
    catchup         = False,
    default_args    = default_args,
    tags            = ['olist', 'dw', 'etl'],
) as dag:

    t_extract    = PythonOperator(task_id='extract_all_sources',  python_callable=extract_all_sources)
    t_dim_date   = PythonOperator(task_id='transform_dim_date',   python_callable=transform_dim_date)
    t_dim_geo    = PythonOperator(task_id='transform_dim_geo',    python_callable=transform_dim_geolocation)
    t_dim_static = PythonOperator(task_id='transform_dim_static', python_callable=transform_dim_static)
    t_dim_cust   = PythonOperator(task_id='transform_dim_customer',python_callable=transform_dim_customer)
    t_dim_sel    = PythonOperator(task_id='transform_dim_seller', python_callable=transform_dim_seller)
    t_dim_prod   = PythonOperator(task_id='transform_dim_product',python_callable=transform_dim_product)
    t_fact_sale  = PythonOperator(task_id='load_fact_sale',       python_callable=load_fact_sale)
    t_fact_del   = PythonOperator(task_id='load_fact_delivery',   python_callable=load_fact_delivery)
    t_fact_rev   = PythonOperator(task_id='load_fact_reviews',    python_callable=load_fact_reviews)
    t_validate   = PythonOperator(task_id='validate_dw',          python_callable=validate_dw)

    # Định nghĩa thứ tự phụ thuộc (dependency graph)
    t_extract >> [t_dim_date, t_dim_geo, t_dim_static, t_dim_prod]
    t_dim_geo  >> [t_dim_cust, t_dim_sel]
    [t_dim_date, t_dim_cust, t_dim_sel, t_dim_prod, t_dim_static] >> t_fact_sale
    [t_dim_date, t_dim_sel,  t_dim_geo,              t_dim_static] >> t_fact_del
    t_fact_sale >> t_fact_rev
    [t_fact_sale, t_fact_del, t_fact_rev] >> t_validate