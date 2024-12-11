import requests
from google.cloud import bigquery
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator 
from datetime import datetime
import json
from datetime import datetime, timedelta

PROJECT_ID = 'bitcoin-438011'
DATASET_ID = 'raw_dataset'
TABLE_ID = 'raw_bitcoin_data'
TEMP_FILE = '/tmp/bitcoin_data.json'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 6),
}

with DAG(
    'bitcoin_data_ingest',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def extract_bitcoin_data():
        url = 'https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=1'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            with open(TEMP_FILE, 'w') as f:
                json.dump(data, f)
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")

    extract_data = PythonOperator(
        task_id='extract_bitcoin_data',
        python_callable=extract_bitcoin_data,
    )

    def transform_and_load_data():
        client = bigquery.Client(project=PROJECT_ID)
        dataset_ref = client.dataset(DATASET_ID)
        table_ref = dataset_ref.table(TABLE_ID)
        schema = [
            bigquery.SchemaField('timestamp', 'TIMESTAMP'),
            bigquery.SchemaField('price_usd', 'FLOAT'),
        ]
        try:
            client.get_table(table_ref)
        except:
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
        with open(TEMP_FILE, 'r') as f:
            data = json.load(f)
        transformed_data = [
            {
                'timestamp': datetime.utcfromtimestamp(price[0] / 1000),
                'price_usd': price[1],
            }
            for price in data['prices']
        ]
        errors = client.insert_rows_json(table_ref, transformed_data)
        if errors:
            raise Exception(f"Failed to insert rows: {errors}")

    transform_and_load_data = PythonOperator(
        task_id='transform_and_load_data',
        python_callable=transform_and_load_data,
    )

    extract_data >> transform_and_load_data
