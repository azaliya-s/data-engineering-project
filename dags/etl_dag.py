import requests
from google.cloud import bigquery
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator 
from datetime import datetime
import json
from datetime import datetime, timedelta


# Ensure credentials are set
if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
    print("Warning: GOOGLE_APPLICATION_CREDENTIALS environment variable not set")

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

    def transform_and_load_data():
        try:
            # Explicitly specify credentials path if needed
            # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/path/to/your/service-account-key.json'
            
            # Initialize BigQuery client with explicit project
            client = bigquery.Client(project=PROJECT_ID)
            
            # Verify project and credentials
            print(f"Connected to project: {client.project}")
            
            dataset_ref = client.dataset(DATASET_ID)
            table_ref = dataset_ref.table(TABLE_ID)
            
            schema = [
                bigquery.SchemaField('timestamp', 'TIMESTAMP'),
                bigquery.SchemaField('price_usd', 'FLOAT'),
            ]
            
            # Create table if it doesn't exist
            try:
                client.get_table(table_ref)
            except Exception as e:
                print(f"Table not found, creating new table: {e}")
                table = bigquery.Table(table_ref, schema=schema)
                client.create_table(table)
            
            # Read and transform data
            with open(TEMP_FILE, 'r') as f:
                data = json.load(f)
            
            transformed_data = [
                {
                    'timestamp': datetime.utcfromtimestamp(price[0] / 1000),
                    'price_usd': price[1],
                }
                for price in data['prices']
            ]
            
            # Insert rows
            errors = client.insert_rows_json(table_ref, transformed_data)
            if errors:
                raise Exception(f"Failed to insert rows: {errors}")
            
            print("Data successfully loaded to BigQuery")
        
        except Exception as e:
            print(f"Error in data transformation and loading: {e}")
            raise

    extract_data = PythonOperator(
        task_id='extract_bitcoin_data',
        python_callable=extract_bitcoin_data,
    )

    transform_and_load_data = PythonOperator(
        task_id='transform_and_load_data',
        python_callable=transform_and_load_data,
    )

    extract_data >> transform_and_load_data
