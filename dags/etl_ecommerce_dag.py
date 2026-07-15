"""
Airflow DAG: ETL E-Commerce Orders
File ini akan diletakkan di folder dags/ Apache Airflow.
Airflow akan otomatis mendeteksi dan menjadwalkannya.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import logging

# Setup logging sederhana
logger = logging.getLogger(__name__)

# === DUMMY FUNCTIONS (Placeholder) ===
# Dalam praktiknya, fungsi-fungsi ini biasanya di-import dari file lain (misal: tasks.py)
def extract_from_source(**kwargs):
    logger.info("Menjalankan task: Extract data dari source...")
    return "Data extracted"

def transform_data(**kwargs):
    logger.info("Menjalankan task: Transform & clean data...")
    return "Data transformed"

def validate_data(**kwargs):
    logger.info("Menjalankan task: Validasi kualitas data...")
    return "Data validated"

def load_to_bigquery(**kwargs):
    logger.info("Menjalankan task: Load ke warehouse...")
    return "Data loaded"

def generate_summary(**kwargs):
    logger.info("Menjalankan task: Generate report...")
    return "Report generated"

def send_slack_alert(**kwargs):
    logger.info("Menjalankan task: Kirim notifikasi...")
    return "Notification sent"

# === DEFAULT ARGS ===
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email': ['alert@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# ============================================================

# Nama fungsi kurang lengkap (contoh: extract_from_source)
# Hal tersebut dapat menyebabkan "Broken DAG".
# 
# Menambahkan 'def' di atas.
# ============================================================

# === DAG DEFINITION ===
with DAG(
    dag_id='etl_ecommerce_daily',
    default_args=default_args,
    description='Daily ETL pipeline untuk data transaksi e-commerce',
    schedule='0 6 * * *',             
    start_date=datetime(2024, 1, 1),
    catchup=False,                    
    tags=['etl', 'ecommerce', 'daily'],
) as dag:

    # Task 1: Start marker
    start = EmptyOperator(task_id='start')

    # Task 2: Extract data dari source
    extract = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_from_source,
    )

    # Task 3: Transform & clean data
    transform = PythonOperator(
        task_id='transform_and_clean',
        python_callable=transform_data,
    )

    # Task 4: Validate data quality
    validate = PythonOperator(
        task_id='validate_quality',
        python_callable=validate_data,
    )

    # Task 5: Load ke warehouse
    load = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_to_bigquery,
    )

    # Task 6: Generate report
    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_summary,
    )

    # Task 7: Send notification
    notify = PythonOperator(
        task_id='send_notification',
        python_callable=send_slack_alert,
    )

    # Task 8: End marker
    end = EmptyOperator(task_id='end')

    # === TASK DEPENDENCIES ===
    start >> extract >> transform >> validate >> load >> report >> notify >> end