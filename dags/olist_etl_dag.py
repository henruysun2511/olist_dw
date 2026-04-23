import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from etl.extract import extract_all_sources
from etl.transform_dims import (
    load_dim_date,
    load_dim_geolocation,
    load_dim_paymenttype,
    load_dim_orderstatus,
    load_dim_customer,
    load_dim_seller,
    load_dim_product,
)
from etl.transform_facts import (
    load_fact_sale,
    load_fact_delivery,
    load_fact_reviews,
)
from etl.validate import validate_dw

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'olist_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Olist E-commerce Data Warehouse',
    schedule=timedelta(days=1),            # ← THÀNH CÁI NÀY
    catchup=False,
) as dag:

    # ── 1. EXTRACT ────────────────────────────────────────────
    t_extract = PythonOperator(
        task_id='extract_from_sqlserver',
        python_callable=extract_all_sources,
    )

    # ── 2. DIM TABLES – nhóm độc lập (không cần geo_key) ─────
    t_dim_date = PythonOperator(
        task_id='load_dim_date',
        python_callable=load_dim_date,
    )
    t_dim_geo = PythonOperator(
        task_id='load_dim_geolocation',
        python_callable=load_dim_geolocation,
    )
    t_dim_pay = PythonOperator(
        task_id='load_dim_paymenttype',
        python_callable=load_dim_paymenttype,
    )
    t_dim_status = PythonOperator(
        task_id='load_dim_orderstatus',
        python_callable=load_dim_orderstatus,
    )

    # ── 3. DIM TABLES – cần geo_key (chạy sau geo) ───────────
    t_dim_customer = PythonOperator(
        task_id='load_dim_customer',
        python_callable=load_dim_customer,
    )
    t_dim_seller = PythonOperator(
        task_id='load_dim_seller',
        python_callable=load_dim_seller,
    )
    t_dim_product = PythonOperator(
        task_id='load_dim_product',
        python_callable=load_dim_product,
    )

    # ── 4. FACT TABLES ────────────────────────────────────────
    t_fact_sale = PythonOperator(
        task_id='load_fact_sale',
        python_callable=load_fact_sale,
    )
    t_fact_delivery = PythonOperator(
        task_id='load_fact_delivery',
        python_callable=load_fact_delivery,
    )
    t_fact_reviews = PythonOperator(
        task_id='load_fact_reviews',
        python_callable=load_fact_reviews,
    )

    # ── 5. VALIDATE ───────────────────────────────────────────
    t_validate = PythonOperator(
        task_id='validate_dw',
        python_callable=validate_dw,
    )

    # ── DEPENDENCY GRAPH ──────────────────────────────────────
    # Extract trước tất cả
    t_extract >> [t_dim_date, t_dim_geo, t_dim_pay, t_dim_status, t_dim_product]

    # Geo phải xong trước Customer và Seller
    t_dim_geo >> [t_dim_customer, t_dim_seller]

    # Tất cả Dim xong mới chạy Fact_Sale
    [t_dim_date, t_dim_customer, t_dim_seller,
     t_dim_product, t_dim_pay, t_dim_status] >> t_fact_sale

    # Fact_Delivery cần Dim_Date, Dim_Seller, Dim_Geo, Dim_Status
    [t_dim_date, t_dim_seller, t_dim_geo, t_dim_status] >> t_fact_delivery

    # Fact_Reviews cần Fact_Sale đã có seller_key/product_key
    t_fact_sale >> t_fact_reviews

    # Validate sau khi cả 3 Fact xong
    [t_fact_sale, t_fact_delivery, t_fact_reviews] >> t_validate