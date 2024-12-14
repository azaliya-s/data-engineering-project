import os 
import requests
import json
from google.cloud import bigquery
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.gcs import GCSHook
from plugins.operators.dbt_operator import DbtRunOperator, DbtTestOperator


# Ensure credentials are set
if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
    print("Warning: GOOGLE_APPLICATION_CREDENTIALS environment variable not set")

PROJECT_ID = 'bitcoin-project-444715'
DATASET_ID = 'raw_dataset'
TABLE_ID = 'raw_bitcoin_data'
TEMP_FILE = '/tmp/bitcoin_data.json'
GCS_BUCKET = 'bitcoin-data-bucket'  
GCS_PATH = 'raw-data/bitcoin_data_{{ ds }}.json' 

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 6),
}

def extract_bitcoin_data():
    url = 'https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=1'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        with open(TEMP_FILE, 'w') as f:
            json.dump(data, f)
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

def upload_to_gcs():
    hook = GCSHook()
    hook.upload(
        bucket_name=GCS_BUCKET,
        object_name='raw-data/bitcoin_data_{{ ds }}.json',
        filename=TEMP_FILE
    )

with DAG(
    'bitcoin_data_ingest_incremental',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # 1. Извлечь данные из API
    extract_data = PythonOperator(
        task_id='extract_bitcoin_data',
        python_callable=extract_bitcoin_data,
    )

    # 2. Загрузить в GCS
    upload_data = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
    )

    # 3. Загрузить из GCS в BigQuery (Load Job)
    # Формируем SQL для загрузки
    # Можно использовать формат JSON и AUTO_DETECT схему, если данные подходят
    load_job = BigQueryInsertJobOperator(
        task_id='load_to_bigquery',
        configuration={
            "load": {
                "sourceUris": [f"gs://{GCS_BUCKET}/{GCS_PATH}"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": TABLE_ID
                },
                "writeDisposition": "WRITE_APPEND",  # Добавляем данные (можно 'WRITE_TRUNCATE' при необходимости)
                "sourceFormat": "NEWLINE_DELIMITED_JSON", # Если нужно, можно преобразовать формат до NLJSON
                "autodetect": True
            }
        }
    )

    # 4. Запустить dbt run (инкрементальная модель)
    dbt_run = DbtRunOperator(
        task_id='dbt_run',
        project_dir='/usr/local/airflow/dbt',
        profiles_dir='/usr/local/airflow/dbt'
    )

    # 5. Запустить dbt test
    dbt_test = DbtTestOperator(
        task_id='dbt_test',
        project_dir='/usr/local/airflow/dbt',
        profiles_dir='/usr/local/airflow/dbt'
    )

    extract_data >> upload_data >> load_job >> dbt_run >> dbt_test
